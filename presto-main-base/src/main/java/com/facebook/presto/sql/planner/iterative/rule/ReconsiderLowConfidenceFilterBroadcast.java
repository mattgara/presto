/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.LocalCostEstimate;
import com.facebook.presto.cost.PlanCostEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableMap;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static com.facebook.presto.SystemSessionProperties.getLowConfidenceFilterBroadcastScaleFactor;
import static com.facebook.presto.SystemSessionProperties.isDefaultFilterFactorEnabled;
import static com.facebook.presto.SystemSessionProperties.isReconsiderLowConfidenceFilterBroadcastEnabled;
import static com.facebook.presto.cost.CostCalculatorWithEstimatedExchanges.calculateJoinCostWithoutOutput;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.LOW;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.planner.iterative.rule.DynamicFilterUtils.addApplicableDynamicFilters;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static java.lang.Double.isFinite;
import static java.lang.Math.abs;
import static java.util.Objects.requireNonNull;

/**
 * Reconsiders only the distribution of a join selected by {@link ReorderJoins}.
 * The join order and build/probe orientation remain unchanged.
 *
 * This rule is deliberately disabled by default. It addresses plans where an
 * unsupported selective filter receives Presto's default 0.9 estimate and the
 * resulting low-confidence build estimate exceeds the ordinary broadcast cap.
 * The configured scale factor supplies a bounded alternate estimate for the
 * cost and cap checks; it is not applied to join enumeration and therefore
 * cannot perturb the selected join order.
 */
public class ReconsiderLowConfidenceFilterBroadcast
        implements Rule<JoinNode>
{
    // FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT is package-private.
    // Matching its exact fallback signature lets this experimental rule target
    // filters whose selectivity came from that heuristic without inspecting a
    // query-specific predicate.
    private static final double UNKNOWN_FILTER_COEFFICIENT = 0.9;
    private static final double FILTER_COEFFICIENT_TOLERANCE = 1e-6;
    private static final Logger log = Logger.get(ReconsiderLowConfidenceFilterBroadcast.class);
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(node -> node.getDistributionType().equals(Optional.of(PARTITIONED)));

    private final CostComparator costComparator;
    private final TaskCountEstimator taskCountEstimator;

    public ReconsiderLowConfidenceFilterBroadcast(CostComparator costComparator, TaskCountEstimator taskCountEstimator)
    {
        this.costComparator = requireNonNull(costComparator, "costComparator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return getJoinDistributionType(session) == AUTOMATIC && isReconsiderLowConfidenceFilterBroadcastEnabled(session);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (joinNode.getType() != INNER || joinNode.getCriteria().isEmpty()) {
            return Result.empty();
        }

        PlanNode build = joinNode.getRight();
        PlanNodeStatsEstimate buildStats = context.getStatsProvider().getStats(build);
        if (buildStats.confidenceLevel() != LOW || !hasUnknownSelectivityFilter(build, context)) {
            return Result.empty();
        }

        double estimatedBuildBytes = buildStats.getOutputSizeInBytes(build);
        double broadcastLimitBytes = getJoinMaxBroadcastTableSize(context.getSession()).toBytes();
        if (!isFinite(estimatedBuildBytes) || estimatedBuildBytes <= broadcastLimitBytes) {
            // ReorderJoins already considered the ordinary replicated candidate.
            return Result.empty();
        }

        double scaleFactor = getLowConfidenceFilterBroadcastScaleFactor(context.getSession());
        double adjustedBuildBytes = estimatedBuildBytes * scaleFactor;
        if (!isFinite(adjustedBuildBytes) || adjustedBuildBytes > broadcastLimitBytes) {
            return Result.empty();
        }

        PlanNodeStatsEstimate adjustedBuildStats = buildStats.mapOutputRowCount(rowCount -> rowCount * scaleFactor);
        StatsProvider adjustedStatsProvider = node -> node.getId().equals(build.getId()) ? adjustedBuildStats : context.getStatsProvider().getStats(node);
        int taskCount = taskCountEstimator.estimateSourceDistributedTaskCount();
        if (taskCount <= 0) {
            return Result.empty();
        }

        LocalCostEstimate partitionedCost = calculateJoinCostWithoutOutput(
                joinNode.getLeft(),
                build,
                adjustedStatsProvider,
                false,
                taskCount);
        LocalCostEstimate replicatedCost = calculateJoinCostWithoutOutput(
                joinNode.getLeft(),
                build,
                adjustedStatsProvider,
                true,
                taskCount);
        PlanCostEstimate partitionedPlanCost = partitionedCost.toPlanCost();
        PlanCostEstimate replicatedPlanCost = replicatedCost.toPlanCost();
        if (partitionedPlanCost.hasUnknownComponents() || replicatedPlanCost.hasUnknownComponents() ||
                costComparator.compare(context.getSession(), replicatedPlanCost, partitionedPlanCost) >= 0) {
            return Result.empty();
        }

        log.info(
                "Reconsidering low-confidence filtered join %s as REPLICATED: estimatedBuildBytes=%.0f, adjustedBuildBytes=%.0f, broadcastLimitBytes=%.0f, taskCount=%s",
                joinNode.getId(),
                estimatedBuildBytes,
                adjustedBuildBytes,
                broadcastLimitBytes,
                taskCount);

        JoinNode replicatedJoin = new JoinNode(
                joinNode.getSourceLocation(),
                joinNode.getId(),
                joinNode.getStatsEquivalentPlanNode(),
                joinNode.getType(),
                joinNode.getLeft(),
                joinNode.getRight(),
                joinNode.getCriteria(),
                joinNode.getOutputVariables(),
                joinNode.getFilter(),
                joinNode.getLeftHashVariable(),
                joinNode.getRightHashVariable(),
                Optional.of(REPLICATED),
                ImmutableMap.of());
        return Result.ofPlanNode(addApplicableDynamicFilters(
                context.getSession(),
                replicatedJoin,
                context.getStatsProvider(),
                context.getIdAllocator()));
    }

    private static boolean hasUnknownSelectivityFilter(PlanNode build, Context context)
    {
        if (!isDefaultFilterFactorEnabled(context.getSession())) {
            return false;
        }
        return searchFrom(build, context.getLookup())
                .where(node -> {
                    if (!(node instanceof FilterNode)) {
                        return false;
                    }
                    FilterNode filter = (FilterNode) node;
                    double filteredRows = context.getStatsProvider().getStats(filter).getOutputRowCount();
                    double sourceRows = context.getStatsProvider().getStats(context.getLookup().resolve(filter.getSource())).getOutputRowCount();
                    if (!isFinite(filteredRows) || !isFinite(sourceRows) || sourceRows <= 0) {
                        return false;
                    }
                    double filterCoefficient = filteredRows / sourceRows;
                    return abs(filterCoefficient - UNKNOWN_FILTER_COEFFICIENT) <= FILTER_COEFFICIENT_TOLERANCE;
                })
                .matches();
    }
}
