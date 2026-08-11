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

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getPushAggregationBelowJoinByteReductionThreshold;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static java.lang.Double.isFinite;

/**
 * Moves a strongly selective dimension join below a grouped aggregation.
 *
 * <pre>
 * Join(Join(fact, dimension), Aggregation(source, key), fact_key = key, residual)
 *     -&gt; Project(Join(fact,
 *             Aggregation(Join(source, AssignUniqueId(dimension)), key, dimension, unique),
 *             fact_key = key,
 *             residual))
 * </pre>
 *
 * A simpler direct join/aggregation form is handled as well. The unique id
 * keeps each moved dimension row in a separate aggregation group. This preserves
 * duplicate-row semantics and does not rely on connector primary-key metadata.
 * A residual top-join filter remains above the aggregation. The rewrite is applied
 * only when statistics show that the moved join materially reduces its fact input.
 */
public class PushSelectiveJoinBelowAggregation
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join()
            .matching(node -> node.getType() == INNER);

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (joinNode.getCriteria().isEmpty() ||
                joinNode.getLeftHashVariable().isPresent() ||
                joinNode.getRightHashVariable().isPresent() ||
                joinNode.getDistributionType().isPresent() ||
                !joinNode.getDynamicFilters().isEmpty()) {
            return Result.empty();
        }

        PlanNode resolvedLeft = context.getLookup().resolve(joinNode.getLeft());
        PlanNode resolvedRight = context.getLookup().resolve(joinNode.getRight());
        boolean aggregationOnLeft = resolvedLeft instanceof AggregationNode;
        boolean aggregationOnRight = resolvedRight instanceof AggregationNode;
        if (aggregationOnLeft == aggregationOnRight) {
            return Result.empty();
        }

        AggregationNode aggregation = (AggregationNode) (aggregationOnLeft ? resolvedLeft : resolvedRight);
        PlanNode aggregationReference = aggregationOnLeft ? joinNode.getLeft() : joinNode.getRight();
        PlanNode otherReference = aggregationOnLeft ? joinNode.getRight() : joinNode.getLeft();
        if (!isSupportedAggregation(aggregation) ||
                !joinUsesOnlyGroupingKeys(joinNode.getCriteria(), aggregation, aggregationOnLeft)) {
            return Result.empty();
        }

        PlanNode resolvedOther = context.getLookup().resolve(otherReference);
        if (resolvedOther instanceof JoinNode) {
            Optional<PlanNode> rewritten = rewriteNestedSelectiveJoin(
                    joinNode,
                    aggregation,
                    aggregationOnLeft,
                    (JoinNode) resolvedOther,
                    context);
            if (rewritten.isPresent()) {
                return Result.ofPlanNode(rewritten.get());
            }
        }

        // A residual filter must stay above the aggregation. The nested-join rewrite
        // handles that case; the direct form is intentionally limited to equi joins.
        if (joinNode.getFilter().isPresent() || !isSelective(joinNode, aggregationReference, context)) {
            return Result.empty();
        }

        List<VariableReferenceExpression> otherOutputs = joinNode.getOutputVariables().stream()
                .filter(otherReference.getOutputVariables()::contains)
                .collect(ImmutableList.toImmutableList());
        if (otherOutputs.stream().anyMatch(variable -> !variable.getType().isComparable())) {
            return Result.empty();
        }

        VariableReferenceExpression unique = context.getVariableAllocator().newVariable("join_row_id", BIGINT);
        AssignUniqueId otherWithUnique = new AssignUniqueId(
                otherReference.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                otherReference,
                unique);

        PlanNode newLeft = aggregationOnLeft ? aggregation.getSource() : otherWithUnique;
        PlanNode newRight = aggregationOnLeft ? otherWithUnique : aggregation.getSource();
        List<VariableReferenceExpression> newJoinOutputs = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(newLeft.getOutputVariables())
                .addAll(newRight.getOutputVariables())
                .build();
        JoinNode newJoin = new JoinNode(
                joinNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                INNER,
                newLeft,
                newRight,
                joinNode.getCriteria(),
                newJoinOutputs,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        List<VariableReferenceExpression> groupingKeys = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(aggregation.getGroupingKeys())
                .addAll(otherOutputs)
                .add(unique)
                .build();
        AggregationNode newAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                newJoin,
                aggregation.getAggregations(),
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                aggregation.getStep(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                newAggregation,
                identityAssignments(joinNode.getOutputVariables())));
    }

    private static Optional<PlanNode> rewriteNestedSelectiveJoin(
            JoinNode topJoin,
            AggregationNode aggregation,
            boolean aggregationOnLeft,
            JoinNode nestedJoin,
            Context context)
    {
        if (nestedJoin.getType() != INNER ||
                nestedJoin.getCriteria().isEmpty() ||
                nestedJoin.getFilter().isPresent() ||
                nestedJoin.getLeftHashVariable().isPresent() ||
                nestedJoin.getRightHashVariable().isPresent() ||
                nestedJoin.getDistributionType().isPresent() ||
                !nestedJoin.getDynamicFilters().isEmpty()) {
            return Optional.empty();
        }

        Optional<PlanNode> rewritten = rewriteNestedSelectiveJoin(
                topJoin,
                aggregation,
                aggregationOnLeft,
                nestedJoin,
                nestedJoin.getLeft(),
                nestedJoin.getRight(),
                true,
                context);
        if (rewritten.isPresent()) {
            return rewritten;
        }
        return rewriteNestedSelectiveJoin(
                topJoin,
                aggregation,
                aggregationOnLeft,
                nestedJoin,
                nestedJoin.getRight(),
                nestedJoin.getLeft(),
                false,
                context);
    }

    private static Optional<PlanNode> rewriteNestedSelectiveJoin(
            JoinNode topJoin,
            AggregationNode aggregation,
            boolean aggregationOnLeft,
            JoinNode nestedJoin,
            PlanNode fact,
            PlanNode dimension,
            boolean factOnLeft,
            Context context)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> factToAggregationKey = new LinkedHashMap<>();
        for (EquiJoinClause clause : topJoin.getCriteria()) {
            VariableReferenceExpression aggregationKey = aggregationOnLeft ? clause.getLeft() : clause.getRight();
            VariableReferenceExpression factKey = aggregationOnLeft ? clause.getRight() : clause.getLeft();
            if (!fact.getOutputVariables().contains(factKey)) {
                return Optional.empty();
            }
            VariableReferenceExpression previous = factToAggregationKey.put(factKey, aggregationKey);
            if (previous != null && !previous.equals(aggregationKey)) {
                return Optional.empty();
            }
        }

        ImmutableList.Builder<EquiJoinClause> pushedCriteria = ImmutableList.builder();
        for (EquiJoinClause clause : nestedJoin.getCriteria()) {
            VariableReferenceExpression factKey = factOnLeft ? clause.getLeft() : clause.getRight();
            VariableReferenceExpression dimensionKey = factOnLeft ? clause.getRight() : clause.getLeft();
            if (!fact.getOutputVariables().contains(factKey) ||
                    !dimension.getOutputVariables().contains(dimensionKey)) {
                return Optional.empty();
            }
            VariableReferenceExpression aggregationKey = factToAggregationKey.get(factKey);
            if (aggregationKey == null || !aggregation.getSource().getOutputVariables().contains(aggregationKey)) {
                return Optional.empty();
            }
            pushedCriteria.add(new EquiJoinClause(aggregationKey, dimensionKey));
        }

        // Measure the selective dimension join itself, not the top residual predicate.
        if (!isSelective(nestedJoin, fact, context)) {
            return Optional.empty();
        }

        ImmutableSet.Builder<VariableReferenceExpression> referencedBuilder = ImmutableSet.builder();
        referencedBuilder.addAll(topJoin.getOutputVariables());
        topJoin.getFilter().ifPresent(filter -> referencedBuilder.addAll(extractUnique(filter)));
        Set<VariableReferenceExpression> referenced = referencedBuilder.build();
        List<VariableReferenceExpression> dimensionOutputs = dimension.getOutputVariables().stream()
                .filter(referenced::contains)
                .collect(ImmutableList.toImmutableList());
        if (dimensionOutputs.stream().anyMatch(variable -> !variable.getType().isComparable())) {
            return Optional.empty();
        }

        VariableReferenceExpression unique = context.getVariableAllocator().newVariable("join_row_id", BIGINT);
        AssignUniqueId dimensionWithUnique = new AssignUniqueId(
                dimension.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                dimension,
                unique);
        JoinNode pushedJoin = new JoinNode(
                topJoin.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                INNER,
                aggregation.getSource(),
                dimensionWithUnique,
                pushedCriteria.build(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(aggregation.getSource().getOutputVariables())
                        .addAll(dimensionWithUnique.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        List<VariableReferenceExpression> groupingKeys = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(aggregation.getGroupingKeys())
                .addAll(dimensionOutputs)
                .add(unique)
                .build();
        AggregationNode pushedAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                pushedJoin,
                aggregation.getAggregations(),
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                aggregation.getStep(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        PlanNode newLeft = aggregationOnLeft ? pushedAggregation : fact;
        PlanNode newRight = aggregationOnLeft ? fact : pushedAggregation;
        ImmutableSet.Builder<VariableReferenceExpression> requiredBuilder = ImmutableSet.builder();
        requiredBuilder.addAll(topJoin.getOutputVariables());
        topJoin.getFilter().ifPresent(filter -> requiredBuilder.addAll(extractUnique(filter)));
        Set<VariableReferenceExpression> required = requiredBuilder.build();
        Set<VariableReferenceExpression> available = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(newLeft.getOutputVariables())
                .addAll(newRight.getOutputVariables())
                .build();
        if (!available.containsAll(required)) {
            return Optional.empty();
        }
        List<VariableReferenceExpression> newTopOutputs = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(newLeft.getOutputVariables().stream()
                        .filter(required::contains)
                        .collect(ImmutableList.toImmutableList()))
                .addAll(newRight.getOutputVariables().stream()
                        .filter(required::contains)
                        .collect(ImmutableList.toImmutableList()))
                .build();
        JoinNode newTopJoin = new JoinNode(
                topJoin.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                INNER,
                newLeft,
                newRight,
                topJoin.getCriteria(),
                newTopOutputs,
                topJoin.getFilter(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        return Optional.of(new ProjectNode(
                context.getIdAllocator().getNextId(),
                newTopJoin,
                identityAssignments(topJoin.getOutputVariables())));
    }

    private static boolean isSupportedAggregation(AggregationNode aggregation)
    {
        return aggregation.getStep() == SINGLE &&
                aggregation.getGroupingSetCount() == 1 &&
                aggregation.getGlobalGroupingSets().isEmpty() &&
                aggregation.getGroupIdVariable().isEmpty() &&
                aggregation.getAggregationId().isEmpty();
    }

    private static boolean joinUsesOnlyGroupingKeys(
            List<EquiJoinClause> criteria,
            AggregationNode aggregation,
            boolean aggregationOnLeft)
    {
        Set<VariableReferenceExpression> groupingKeys = ImmutableSet.copyOf(aggregation.getGroupingKeys());
        return criteria.stream()
                .map(clause -> aggregationOnLeft ? clause.getLeft() : clause.getRight())
                .allMatch(groupingKeys::contains);
    }

    private static boolean isSelective(JoinNode joinNode, PlanNode aggregationReference, Context context)
    {
        StatsProvider stats = context.getStatsProvider();
        PlanNodeStatsEstimate joinStats = stats.getStats(joinNode);
        PlanNodeStatsEstimate aggregationStats = stats.getStats(aggregationReference);
        double joinBytes = joinStats.getOutputSizeInBytes(joinNode);
        double aggregationBytes = aggregationStats.getOutputSizeInBytes(aggregationReference);
        double threshold = getPushAggregationBelowJoinByteReductionThreshold(context.getSession());
        if (isFinite(joinBytes) && isFinite(aggregationBytes) && aggregationBytes > 0) {
            return joinBytes <= aggregationBytes * threshold;
        }

        // An aggregate output often has unknown average widths even when
        // both cardinalities are known. Fall back to the equivalent row-count gate.
        double joinRows = joinStats.getOutputRowCount();
        double aggregationRows = aggregationStats.getOutputRowCount();
        return isFinite(joinRows) &&
                isFinite(aggregationRows) &&
                aggregationRows > 0 &&
                joinRows <= aggregationRows * threshold;
    }
}
