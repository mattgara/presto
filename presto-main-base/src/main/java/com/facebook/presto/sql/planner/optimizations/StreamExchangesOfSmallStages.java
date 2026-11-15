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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CachingStatsProvider;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getExchangeMaterializationMemoryThreshold;
import static com.facebook.presto.SystemSessionProperties.getExchangeMaterializationStrategy;
import static com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy.AUTOMATIC;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Materializes exchanges only into the stages that need it. With the AUTOMATIC exchange materialization strategy,
 * AddExchanges materializes the same exchanges as ALL. This optimizer then streams the materialized exchanges into
 * every stage whose estimated memory per worker is at most exchange_materialization_memory_threshold. A stage is the
 * part of the plan between remote exchanges. Its memory is the state of its operators: join and semi join build
 * sides, grouped final aggregations, and the inputs of window, row number and mark distinct. The partitioned part
 * is divided among the workers, the replicated part is not. A stage without an estimate keeps its exchanges
 * materialized. The exchanges streamed into a stage are also partitioned as without materialization (system hash
 * partitioning) when all of the stage's repartitioning inputs stream and the stage neither reads nor writes a table,
 * whose connector partitioning the stage could depend on.
 */
public class StreamExchangesOfSmallStages
        implements PlanOptimizer
{
    private final StatsCalculator statsCalculator;
    private final TaskCountEstimator taskCountEstimator;

    public StreamExchangesOfSmallStages(StatsCalculator statsCalculator, TaskCountEstimator taskCountEstimator)
    {
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public boolean isEnabled(Session session, boolean forceEnableOptimizer)
    {
        return getExchangeMaterializationStrategy(session) == AUTOMATIC;
    }

    @Override
    public boolean isCostBased(Session session)
    {
        return isEnabled(session, false);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator, WarningCollector warningCollector, boolean forceEnableOptimizer)
    {
        if (!isEnabled(session, forceEnableOptimizer)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        StageMemory stageMemory = new StageMemory(
                new CachingStatsProvider(statsCalculator, session, types),
                taskCountEstimator.estimateHashedTaskCount(session),
                getExchangeMaterializationMemoryThreshold(session).toBytes());
        Set<PlanNodeId> streamed = new HashSet<>();
        Set<PlanNodeId> systemPartitioned = new HashSet<>();
        stageMemory.collectExchangesToStream(plan, streamed, systemPartitioned);
        if (streamed.isEmpty()) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        return PlanOptimizerResult.optimizerResult(SimplePlanRewriter.rewriteWith(new Streamer(streamed, systemPartitioned), plan), true);
    }

    private static class StageMemory
    {
        private final StatsProvider statsProvider;
        private final int workers;
        private final double threshold;

        private StageMemory(StatsProvider statsProvider, int workers, double threshold)
        {
            this.statsProvider = requireNonNull(statsProvider, "statsProvider is null");
            this.workers = workers;
            this.threshold = threshold;
        }

        // Decides for the stage rooted at stageRoot, then for the stages that feed it.
        private void collectExchangesToStream(PlanNode stageRoot, Set<PlanNodeId> streamed, Set<PlanNodeId> systemPartitioned)
        {
            List<ExchangeNode> inputs = new ArrayList<>();
            double partitionedBytes = 0;
            double replicatedBytes = 0;
            boolean usesTable = false;
            Deque<PlanNode> pending = new ArrayDeque<>();
            pending.push(stageRoot);
            while (!pending.isEmpty()) {
                PlanNode node = pending.pop();
                if (node instanceof ExchangeNode && ((ExchangeNode) node).getScope().isRemote()) {
                    inputs.add((ExchangeNode) node);
                    continue;
                }
                usesTable |= node instanceof TableScanNode || node instanceof TableWriterNode;
                if (isReplicated(node)) {
                    replicatedBytes += stateBytes(node);
                }
                else {
                    partitionedBytes += stateBytes(node);
                }
                node.getSources().forEach(pending::push);
            }
            // An unknown estimate is NaN, which fails the comparison.
            if (partitionedBytes / workers + replicatedBytes <= threshold) {
                inputs.stream()
                        .filter(input -> input.getScope() == REMOTE_MATERIALIZED)
                        .forEach(input -> streamed.add(input.getId()));
                List<ExchangeNode> repartitions = inputs.stream()
                        .filter(input -> input.getType() == REPARTITION)
                        .collect(toImmutableList());
                if (!usesTable && repartitions.stream().allMatch(input -> input.getScope() == REMOTE_MATERIALIZED)) {
                    repartitions.forEach(input -> systemPartitioned.add(input.getId()));
                }
            }
            inputs.forEach(input -> input.getSources().forEach(source -> collectExchangesToStream(source, streamed, systemPartitioned)));
        }

        private double stateBytes(PlanNode node)
        {
            if (node instanceof JoinNode) {
                return outputBytes(((JoinNode) node).getRight());
            }
            if (node instanceof SemiJoinNode) {
                return outputBytes(((SemiJoinNode) node).getFilteringSource());
            }
            if (node instanceof AggregationNode) {
                AggregationNode aggregation = (AggregationNode) node;
                boolean holdsGroups = (aggregation.getStep() == FINAL || aggregation.getStep() == SINGLE) && !aggregation.getGroupingKeys().isEmpty();
                return holdsGroups ? outputBytes(aggregation) : 0;
            }
            if (node instanceof WindowNode || node instanceof RowNumberNode || node instanceof MarkDistinctNode) {
                return outputBytes(node.getSources().get(0));
            }
            return 0;
        }

        private static boolean isReplicated(PlanNode node)
        {
            if (node instanceof JoinNode) {
                return ((JoinNode) node).getDistributionType().map(REPLICATED::equals).orElse(false);
            }
            if (node instanceof SemiJoinNode) {
                return ((SemiJoinNode) node).getDistributionType().map(SemiJoinNode.DistributionType.REPLICATED::equals).orElse(false);
            }
            return false;
        }

        private double outputBytes(PlanNode node)
        {
            return statsProvider.getStats(node).getOutputSizeInBytes(node);
        }
    }

    private static class Streamer
            extends SimplePlanRewriter<Void>
    {
        private final Set<PlanNodeId> streamed;
        private final Set<PlanNodeId> systemPartitioned;

        private Streamer(Set<PlanNodeId> streamed, Set<PlanNodeId> systemPartitioned)
        {
            this.streamed = requireNonNull(streamed, "streamed is null");
            this.systemPartitioned = requireNonNull(systemPartitioned, "systemPartitioned is null");
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<Void> context)
        {
            ExchangeNode rewritten = (ExchangeNode) context.defaultRewrite(node);
            if (!streamed.contains(node.getId())) {
                return rewritten;
            }
            return new ExchangeNode(
                    rewritten.getSourceLocation(),
                    rewritten.getId(),
                    rewritten.getStatsEquivalentPlanNode(),
                    rewritten.getType(),
                    REMOTE_STREAMING,
                    systemPartitioned.contains(node.getId()) ? withSystemPartitioning(rewritten.getPartitioningScheme()) : rewritten.getPartitioningScheme(),
                    rewritten.getSources(),
                    rewritten.getInputs(),
                    rewritten.isEnsureSourceOrdering(),
                    rewritten.getOrderingScheme());
        }

        private static PartitioningScheme withSystemPartitioning(PartitioningScheme scheme)
        {
            return new PartitioningScheme(
                    new Partitioning(FIXED_HASH_DISTRIBUTION, scheme.getPartitioning().getArguments()),
                    scheme.getOutputLayout(),
                    scheme.getHashColumn(),
                    scheme.isReplicateNullsAndAny(),
                    scheme.isScaleWriters(),
                    scheme.getEncoding(),
                    scheme.getBucketToPartition());
        }
    }
}
