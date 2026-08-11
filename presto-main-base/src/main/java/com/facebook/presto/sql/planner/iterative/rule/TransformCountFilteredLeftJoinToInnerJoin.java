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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;
import static java.util.Objects.requireNonNull;

/**
 * Converts a LEFT join below an aggregation to an INNER join when a HAVING-style
 * predicate requires {@code count(right_side_variable) > 0}.
 *
 * An unmatched left row contributes zero to such a count and is necessarily
 * removed by the filter. Removing it at the join is therefore equivalent and
 * exposes an inner-join graph to join reordering. COUNT(*) is deliberately not
 * eligible because an unmatched left row contributes one to COUNT(*).
 */
public class TransformCountFilteredLeftJoinToInnerJoin
        implements Rule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;

    public TransformCountFilteredLeftJoinToInnerJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        List<ProjectNode> projectChain = new ArrayList<>();
        PlanNode current = context.getLookup().resolve(filterNode.getSource());
        while (current instanceof ProjectNode) {
            projectChain.add((ProjectNode) current);
            current = context.getLookup().resolve(((ProjectNode) current).getSource());
        }
        if (!(current instanceof AggregationNode)) {
            return Result.empty();
        }

        AggregationNode aggregationNode = (AggregationNode) current;
        PlanNode aggregationSource = context.getLookup().resolve(aggregationNode.getSource());
        if (!(aggregationSource instanceof JoinNode)) {
            return Result.empty();
        }

        JoinNode joinNode = (JoinNode) aggregationSource;
        if (joinNode.getType() != JoinType.LEFT) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> leftOutputs = ImmutableSet.copyOf(joinNode.getLeft().getOutputVariables());
        Set<VariableReferenceExpression> rightOutputs = ImmutableSet.copyOf(joinNode.getRight().getOutputVariables());
        if (!hasPositiveRightCountFilter(filterNode, projectChain, aggregationNode, leftOutputs, rightOutputs)) {
            return Result.empty();
        }

        JoinNode innerJoin = new JoinNode(
                joinNode.getSourceLocation(),
                joinNode.getId(),
                JoinType.INNER,
                joinNode.getLeft(),
                joinNode.getRight(),
                joinNode.getCriteria(),
                joinNode.getOutputVariables(),
                joinNode.getFilter(),
                joinNode.getLeftHashVariable(),
                joinNode.getRightHashVariable(),
                joinNode.getDistributionType(),
                joinNode.getDynamicFilters());

        PlanNode rewritten = aggregationNode.replaceChildren(ImmutableList.of(innerJoin));
        for (int i = projectChain.size() - 1; i >= 0; i--) {
            rewritten = projectChain.get(i).replaceChildren(ImmutableList.of(rewritten));
        }
        return Result.ofPlanNode(filterNode.replaceChildren(ImmutableList.of(rewritten)));
    }

    private boolean hasPositiveRightCountFilter(
            FilterNode filterNode,
            List<ProjectNode> projectChain,
            AggregationNode aggregationNode,
            Set<VariableReferenceExpression> leftOutputs,
            Set<VariableReferenceExpression> rightOutputs)
    {
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
            VariableReferenceExpression countOutput = entry.getKey();
            Aggregation aggregation = entry.getValue();
            if (!isEligibleRightCount(aggregation, leftOutputs, rightOutputs) ||
                    !isPassedThroughByIdentityProjects(countOutput, projectChain)) {
                continue;
            }
            if (extractConjuncts(filterNode.getPredicate()).stream()
                    .anyMatch(conjunct -> isGreaterThanZero(conjunct, countOutput))) {
                return true;
            }
        }
        return false;
    }

    private boolean isEligibleRightCount(
            Aggregation aggregation,
            Set<VariableReferenceExpression> leftOutputs,
            Set<VariableReferenceExpression> rightOutputs)
    {
        if (!functionResolution.isCountFunction(aggregation.getFunctionHandle()) ||
                aggregation.isDistinct() ||
                aggregation.getFilter().isPresent() ||
                aggregation.getMask().isPresent() ||
                aggregation.getOrderBy().isPresent() ||
                aggregation.getArguments().size() != 1 ||
                !(aggregation.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return false;
        }
        VariableReferenceExpression argument = (VariableReferenceExpression) aggregation.getArguments().get(0);
        return rightOutputs.contains(argument) && !leftOutputs.contains(argument);
    }

    private static boolean isPassedThroughByIdentityProjects(
            VariableReferenceExpression variable,
            List<ProjectNode> projectChain)
    {
        for (ProjectNode projectNode : projectChain) {
            if (!variable.equals(projectNode.getAssignments().get(variable))) {
                return false;
            }
        }
        return true;
    }

    private boolean isGreaterThanZero(RowExpression expression, VariableReferenceExpression countOutput)
    {
        if (!(expression instanceof CallExpression)) {
            return false;
        }
        CallExpression call = (CallExpression) expression;
        if (call.getArguments().size() != 2 || !functionResolution.isComparisonFunction(call.getFunctionHandle())) {
            return false;
        }
        Optional<OperatorType> operator = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getOperatorType();
        if (!operator.isPresent()) {
            return false;
        }

        RowExpression left = call.getArguments().get(0);
        RowExpression right = call.getArguments().get(1);
        OperatorType normalizedOperator;
        RowExpression constant;
        if (countOutput.equals(left)) {
            normalizedOperator = operator.get();
            constant = right;
        }
        else if (countOutput.equals(right)) {
            normalizedOperator = OperatorType.flip(operator.get());
            constant = left;
        }
        else {
            return false;
        }

        return normalizedOperator == GREATER_THAN &&
                constant instanceof ConstantExpression &&
                Long.valueOf(0).equals(((ConstantExpression) constant).getValue());
    }
}
