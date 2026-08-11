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

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExistsExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.plan.AggregationNode.globalAggregation;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.LateralJoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.Objects.requireNonNull;

/**
 * Combines adjacent correlated EXISTS applications when they scan the same
 * relation and one predicate is a strict conjunctive refinement of the other.
 *
 * For example, Q21 contains:
 *
 * <pre>
 * EXISTS (SELECT * FROM lineitem l2 WHERE K(l2, outer))
 * AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE K(l3, outer) AND E(l3))
 * </pre>
 *
 * The ordinary EXISTS rewrite evaluates these as two independent correlated
 * aggregations. This rule evaluates K once and computes both the broad match
 * count and a flag for E in the same aggregation. It preserves both boolean
 * outputs; the enclosing expression still decides whether either output is
 * negated.
 *
 * The deliberately narrow shape and determinism checks make this an optional
 * optimization: plans that cannot be proven equivalent are left unchanged for
 * the existing single-EXISTS rule.
 */
public class TransformPairedExistsApplyToLateralNode
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode();

    private final FunctionAndTypeManager functionAndTypeManager;
    private final StandardFunctionResolution functionResolution;
    private final LogicalRowExpressions logicalRowExpressions;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public TransformPairedExistsApplyToLateralNode(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
        this.logicalRowExpressions = new LogicalRowExpressions(
                determinismEvaluator,
                functionResolution,
                functionAndTypeManager);
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode outer, Captures captures, Context context)
    {
        PlanNode resolvedInput = context.getLookup().resolve(outer.getInput());
        if (!(resolvedInput instanceof ApplyNode)) {
            return Result.empty();
        }

        ApplyNode inner = (ApplyNode) resolvedInput;
        Optional<ExistsInput> innerExists = extractExistsInput(inner, context.getLookup());
        Optional<ExistsInput> outerExists = extractExistsInput(outer, context.getLookup());
        if (!innerExists.isPresent() || !outerExists.isPresent()) {
            return Result.empty();
        }

        if (inner.getCorrelation().isEmpty() ||
                !ImmutableSet.copyOf(inner.getCorrelation()).equals(ImmutableSet.copyOf(outer.getCorrelation()))) {
            return Result.empty();
        }

        Optional<PairedInputs> pairedInputs = findBroadAndNarrow(innerExists.get(), outerExists.get());
        if (!pairedInputs.isPresent()) {
            return Result.empty();
        }

        PairedInputs pair = pairedInputs.get();
        Set<VariableReferenceExpression> allowedPredicateVariables = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(pair.getNarrow().getTableScan().getOutputVariables())
                .addAll(inner.getCorrelation())
                .build();
        if (!allowedPredicateVariables.containsAll(VariablesExtractor.extractUnique(pair.getBroadPredicateInNarrowVariables()))) {
            return Result.empty();
        }

        RowExpression extraPredicate = logicalRowExpressions.combineConjuncts(pair.getExtraConjuncts());
        Set<VariableReferenceExpression> extraVariables = VariablesExtractor.extractUnique(extraPredicate);
        if (!ImmutableSet.copyOf(pair.getNarrow().getTableScan().getOutputVariables()).containsAll(extraVariables) ||
                ImmutableSet.copyOf(inner.getCorrelation()).stream().anyMatch(extraVariables::contains)) {
            return Result.empty();
        }

        VariableReferenceExpression extraFlag = context.getVariableAllocator().newVariable(
                extraPredicate.getSourceLocation(),
                "paired_exists_extra",
                BIGINT);
        Assignments flaggedAssignments = Assignments.builder()
                .putAll(identityAssignments(pair.getNarrow().getTableScan().getOutputVariables()))
                .put(extraFlag, specialForm(
                        IF,
                        BIGINT,
                        ImmutableList.of(
                                extraPredicate,
                                new ConstantExpression(1L, BIGINT),
                                new ConstantExpression(0L, BIGINT))))
                .build();

        PlanNode sharedSource = new ProjectNode(
                context.getIdAllocator().getNextId(),
                new FilterNode(
                        pair.getBroad().getFilter().getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        pair.getNarrow().getTableScan(),
                        pair.getBroadPredicateInNarrowVariables()),
                flaggedAssignments);

        VariableReferenceExpression broadCount = context.getVariableAllocator().newVariable("paired_exists_count", BIGINT);
        VariableReferenceExpression narrowMaximum = context.getVariableAllocator().newVariable("paired_exists_max", BIGINT);
        AggregationNode aggregation = new AggregationNode(
                outer.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                sharedSource,
                ImmutableMap.of(
                        broadCount,
                        new Aggregation(
                                new CallExpression(
                                        broadCount.getSourceLocation(),
                                        "count",
                                        functionResolution.countFunction(),
                                        BIGINT,
                                        ImmutableList.of()),
                                Optional.empty(),
                                Optional.empty(),
                                false,
                                Optional.empty()),
                        narrowMaximum,
                        new Aggregation(
                                new CallExpression(
                                        narrowMaximum.getSourceLocation(),
                                        "max",
                                        functionAndTypeManager.lookupFunction("max", fromTypes(BIGINT)),
                                        BIGINT,
                                        ImmutableList.of(extraFlag)),
                                Optional.empty(),
                                Optional.empty(),
                                false,
                                Optional.empty())),
                globalAggregation(),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        RowExpression broadResult = comparisonExpression(
                functionResolution,
                GREATER_THAN,
                broadCount,
                new ConstantExpression(0L, BIGINT));
        RowExpression narrowResult = comparisonExpression(
                functionResolution,
                GREATER_THAN,
                specialForm(
                        COALESCE,
                        BIGINT,
                        ImmutableList.of(narrowMaximum, new ConstantExpression(0L, BIGINT))),
                new ConstantExpression(0L, BIGINT));

        Map<VariableReferenceExpression, RowExpression> resultByVariable = ImmutableMap.of(
                pair.getBroad().getExistsVariable(), broadResult,
                pair.getNarrow().getExistsVariable(), narrowResult);
        Assignments.Builder resultAssignments = Assignments.builder();
        resultAssignments.put(
                innerExists.get().getExistsVariable(),
                resultByVariable.get(innerExists.get().getExistsVariable()));
        resultAssignments.put(
                outerExists.get().getExistsVariable(),
                resultByVariable.get(outerExists.get().getExistsVariable()));

        PlanNode subquery = new ProjectNode(
                context.getIdAllocator().getNextId(),
                aggregation,
                resultAssignments.build());

        return Result.ofPlanNode(new LateralJoinNode(
                outer.getSourceLocation(),
                outer.getId(),
                inner.getInput(),
                subquery,
                inner.getCorrelation(),
                INNER,
                outer.getOriginSubqueryError()));
    }

    private Optional<ExistsInput> extractExistsInput(ApplyNode applyNode, Lookup lookup)
    {
        if (applyNode.getSubqueryAssignments().size() != 1) {
            return Optional.empty();
        }
        RowExpression assignment = applyNode.getSubqueryAssignments().getExpressions().stream().collect(onlyElement());
        if (!(assignment instanceof ExistsExpression)) {
            return Optional.empty();
        }

        PlanNode resolvedSubquery = lookup.resolve(applyNode.getSubquery());
        if (!(resolvedSubquery instanceof ProjectNode) || !resolvedSubquery.getOutputVariables().isEmpty()) {
            return Optional.empty();
        }

        PlanNode projectSource = lookup.resolve(((ProjectNode) resolvedSubquery).getSource());
        if (!(projectSource instanceof FilterNode)) {
            return Optional.empty();
        }
        FilterNode filter = (FilterNode) projectSource;
        PlanNode filterSource = lookup.resolve(filter.getSource());
        if (!(filterSource instanceof TableScanNode) || !determinismEvaluator.isDeterministic(filter.getPredicate())) {
            return Optional.empty();
        }

        return Optional.of(new ExistsInput(
                applyNode.getSubqueryAssignments().getVariables().stream().collect(onlyElement()),
                filter,
                (TableScanNode) filterSource));
    }

    private Optional<PairedInputs> findBroadAndNarrow(ExistsInput first, ExistsInput second)
    {
        Optional<PairedInputs> firstBroad = tryPair(first, second);
        if (firstBroad.isPresent()) {
            return firstBroad;
        }
        return tryPair(second, first);
    }

    private Optional<PairedInputs> tryPair(ExistsInput broad, ExistsInput narrow)
    {
        if (!broad.getTableScan().getTable().equals(narrow.getTableScan().getTable()) ||
                broad.getTableScan().getTable().getLayout().isPresent() ||
                narrow.getTableScan().getTable().getLayout().isPresent() ||
                !broad.getTableScan().getCurrentConstraint().isAll() ||
                !narrow.getTableScan().getCurrentConstraint().isAll() ||
                !broad.getTableScan().getEnforcedConstraint().isAll() ||
                !narrow.getTableScan().getEnforcedConstraint().isAll()) {
            return Optional.empty();
        }

        Map<ColumnHandle, VariableReferenceExpression> narrowVariablesByColumn = new LinkedHashMap<>();
        for (VariableReferenceExpression variable : narrow.getTableScan().getOutputVariables()) {
            ColumnHandle column = narrow.getTableScan().getAssignments().get(variable);
            if (column == null || narrowVariablesByColumn.put(column, variable) != null) {
                return Optional.empty();
            }
        }

        Map<VariableReferenceExpression, RowExpression> broadToNarrowVariables = new LinkedHashMap<>();
        for (VariableReferenceExpression variable : broad.getTableScan().getOutputVariables()) {
            ColumnHandle column = broad.getTableScan().getAssignments().get(variable);
            VariableReferenceExpression narrowVariable = narrowVariablesByColumn.get(column);
            if (narrowVariable == null || !variable.getType().equals(narrowVariable.getType())) {
                return Optional.empty();
            }
            broadToNarrowVariables.put(variable, narrowVariable);
        }

        RowExpression broadPredicate = RowExpressionVariableInliner.inlineVariables(
                variable -> broadToNarrowVariables.getOrDefault(variable, variable),
                broad.getFilter().getPredicate());
        Set<RowExpression> broadConjuncts = new LinkedHashSet<>(extractConjuncts(broadPredicate));
        Set<RowExpression> narrowConjuncts = new LinkedHashSet<>(extractConjuncts(narrow.getFilter().getPredicate()));
        if (broadConjuncts.isEmpty() ||
                !narrowConjuncts.containsAll(broadConjuncts) ||
                narrowConjuncts.size() == broadConjuncts.size()) {
            return Optional.empty();
        }

        Set<RowExpression> extraConjuncts = new LinkedHashSet<>(narrowConjuncts);
        extraConjuncts.removeAll(broadConjuncts);
        if (!extraConjuncts.stream().allMatch(determinismEvaluator::isDeterministic)) {
            return Optional.empty();
        }

        return Optional.of(new PairedInputs(
                broad,
                narrow,
                logicalRowExpressions.combineConjuncts(broadConjuncts),
                ImmutableList.copyOf(extraConjuncts)));
    }

    private static final class ExistsInput
    {
        private final VariableReferenceExpression existsVariable;
        private final FilterNode filter;
        private final TableScanNode tableScan;

        private ExistsInput(VariableReferenceExpression existsVariable, FilterNode filter, TableScanNode tableScan)
        {
            this.existsVariable = requireNonNull(existsVariable, "existsVariable is null");
            this.filter = requireNonNull(filter, "filter is null");
            this.tableScan = requireNonNull(tableScan, "tableScan is null");
        }

        private VariableReferenceExpression getExistsVariable()
        {
            return existsVariable;
        }

        private FilterNode getFilter()
        {
            return filter;
        }

        private TableScanNode getTableScan()
        {
            return tableScan;
        }
    }

    private static final class PairedInputs
    {
        private final ExistsInput broad;
        private final ExistsInput narrow;
        private final RowExpression broadPredicateInNarrowVariables;
        private final List<RowExpression> extraConjuncts;

        private PairedInputs(
                ExistsInput broad,
                ExistsInput narrow,
                RowExpression broadPredicateInNarrowVariables,
                List<RowExpression> extraConjuncts)
        {
            this.broad = requireNonNull(broad, "broad is null");
            this.narrow = requireNonNull(narrow, "narrow is null");
            this.broadPredicateInNarrowVariables = requireNonNull(broadPredicateInNarrowVariables, "broadPredicateInNarrowVariables is null");
            this.extraConjuncts = ImmutableList.copyOf(requireNonNull(extraConjuncts, "extraConjuncts is null"));
        }

        private ExistsInput getBroad()
        {
            return broad;
        }

        private ExistsInput getNarrow()
        {
            return narrow;
        }

        private RowExpression getBroadPredicateInNarrowVariables()
        {
            return broadPredicateInNarrowVariables;
        }

        private List<RowExpression> getExtraConjuncts()
        {
            return extraConjuncts;
        }
    }
}
