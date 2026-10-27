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
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.PUSH_SELECTIVE_JOIN_BELOW_AGGREGATION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.assignUniqueId;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushSelectiveJoinBelowAggregation
        extends BaseRuleTest
{
    @Test
    public void testPushesSelectiveJoinBelowAggregation()
    {
        assertThatRule()
                .overrideStats("join", rows(10))
                .overrideStats("aggregation", rows(1000))
                .on(p -> directPlan(p, Optional.empty(), false))
                .matches(project(
                        aggregation(
                                singleGroupingSet("fact_key", "dimension_key", "dimension_value", "unique"),
                                ImmutableMap.of(Optional.of("average"), functionCall("avg", ImmutableList.of("fact_value"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("dimension_key", "fact_key")),
                                        assignUniqueId("unique", values("dimension_key", "dimension_value")),
                                        values("fact_key", "fact_value")))));
    }

    @Test
    public void testPushesNestedSelectiveJoinBelowAggregation()
    {
        // Join(Join(fact, dimension), Aggregation(source)) as in TPC-H Q17: the dimension join
        // moves below the aggregation and the residual filter stays on the top join.
        assertThatRule()
                .overrideStats("nested", rows(10))
                .overrideStats("fact", rows(1000))
                .on(TestPushSelectiveJoinBelowAggregation::nestedPlan)
                .matches(project(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("fact_key", "source_key")),
                                Optional.of("quantity < average"),
                                values("fact_key", "quantity"),
                                aggregation(
                                        singleGroupingSet("source_key", "unique"),
                                        ImmutableMap.of(Optional.of("average"), functionCall("avg", ImmutableList.of("source_quantity"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        join(
                                                INNER,
                                                ImmutableList.of(equiJoinClause("source_key", "dimension_key")),
                                                values("source_key", "source_quantity"),
                                                assignUniqueId("unique", values("dimension_key")))))));
    }

    @Test
    public void testDoesNotPushJoinThatIsNotSelective()
    {
        // The join output (three columns) has more bytes than the aggregation output (two columns).
        assertThatRule()
                .overrideStats("join", rows(1000))
                .overrideStats("aggregation", rows(1000))
                .on(p -> directPlan(p, Optional.empty(), false))
                .doesNotFire();
    }

    @Test
    public void testThreshold()
    {
        assertThatRule()
                .setSystemProperty(PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD, "2")
                .overrideStats("join", rows(1000))
                .overrideStats("aggregation", rows(1000))
                .on(p -> directPlan(p, Optional.empty(), false))
                .matches(project(aggregation(ImmutableMap.of(), join(assignUniqueId("unique", values("dimension_key", "dimension_value")), values("fact_key", "fact_value")))));
    }

    @Test
    public void testDoesNotPushWithoutStatistics()
    {
        assertThatRule()
                .overrideStats("join", PlanNodeStatsEstimate.unknown())
                .overrideStats("aggregation", PlanNodeStatsEstimate.unknown())
                .on(p -> directPlan(p, Optional.empty(), false))
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushJoinOnAggregateOutput()
    {
        assertThatRule()
                .overrideStats("join", rows(10))
                .overrideStats("aggregation", rows(1000))
                .on(p -> directPlan(p, Optional.empty(), true))
                .doesNotFire();
    }

    @Test
    public void testKeepsDirectJoinFilterAboveAggregation()
    {
        // The filter reads the aggregate, so it stays above the aggregation. The aggregation
        // groups on dimension_value, which only the filter reads.
        assertThatRule()
                .overrideStats("join", rows(10))
                .overrideStats("aggregation", rows(1000))
                .on(p -> directPlan(p, Optional.of("dimension_value < average"), false))
                .matches(project(
                        filter(
                                "dimension_value < average",
                                aggregation(
                                        singleGroupingSet("fact_key", "dimension_key", "dimension_value", "unique"),
                                        ImmutableMap.of(Optional.of("average"), functionCall("avg", ImmutableList.of("fact_value"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        join(
                                                INNER,
                                                ImmutableList.of(equiJoinClause("dimension_key", "fact_key")),
                                                assignUniqueId("unique", values("dimension_key", "dimension_value")),
                                                values("fact_key", "fact_value"))))));
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new PushSelectiveJoinBelowAggregation())
                .overrideStats("join", rows(10))
                .overrideStats("aggregation", rows(1000))
                .on(p -> directPlan(p, Optional.empty(), false))
                .doesNotFire();
    }

    private RuleAssert assertThatRule()
    {
        return tester().assertThat(new PushSelectiveJoinBelowAggregation())
                .setSystemProperty(PUSH_SELECTIVE_JOIN_BELOW_AGGREGATION, "true");
    }

    private static PlanNodeStatsEstimate rows(double rowCount)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount)
                .build();
    }

    // Join(dimension, Aggregation(fact)) on dimension_key = fact_key, or on dimension_value = average.
    // With a filter, the join does not output dimension_value.
    private static PlanNode directPlan(PlanBuilder p, Optional<String> filter, boolean joinOnAggregate)
    {
        VariableReferenceExpression factKey = p.variable("fact_key", BIGINT);
        VariableReferenceExpression factValue = p.variable("fact_value", DOUBLE);
        VariableReferenceExpression dimensionKey = p.variable("dimension_key", BIGINT);
        VariableReferenceExpression dimensionValue = p.variable("dimension_value", DOUBLE);
        VariableReferenceExpression average = p.variable("average", DOUBLE);

        PlanNode aggregation = p.aggregation(builder -> builder
                .setPlanNodeId(new PlanNodeId("aggregation"))
                .source(p.values(factKey, factValue))
                .addAggregation(average, p.rowExpression("avg(fact_value)"))
                .singleGroupingSet(factKey));
        return new JoinNode(
                Optional.empty(),
                new PlanNodeId("join"),
                INNER,
                p.values(dimensionKey, dimensionValue),
                aggregation,
                ImmutableList.of(joinOnAggregate ? new EquiJoinClause(dimensionValue, average) : new EquiJoinClause(dimensionKey, factKey)),
                filter.isPresent() ? ImmutableList.of(dimensionKey, average) : ImmutableList.of(dimensionKey, dimensionValue, average),
                filter.map(p::rowExpression),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    // Join(Join(fact, dimension), Aggregation(source)) on fact_key = source_key, filtered on quantity < average.
    private static PlanNode nestedPlan(PlanBuilder p)
    {
        VariableReferenceExpression factKey = p.variable("fact_key", BIGINT);
        VariableReferenceExpression quantity = p.variable("quantity", DOUBLE);
        VariableReferenceExpression dimensionKey = p.variable("dimension_key", BIGINT);
        VariableReferenceExpression sourceKey = p.variable("source_key", BIGINT);
        VariableReferenceExpression sourceQuantity = p.variable("source_quantity", DOUBLE);
        VariableReferenceExpression average = p.variable("average", DOUBLE);

        JoinNode nested = new JoinNode(
                Optional.empty(),
                new PlanNodeId("nested"),
                INNER,
                p.values(new PlanNodeId("fact"), factKey, quantity),
                p.values(dimensionKey),
                ImmutableList.of(new EquiJoinClause(factKey, dimensionKey)),
                ImmutableList.of(factKey, quantity, dimensionKey),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
        PlanNode aggregation = p.aggregation(builder -> builder
                .source(p.values(sourceKey, sourceQuantity))
                .addAggregation(average, p.rowExpression("avg(source_quantity)"))
                .singleGroupingSet(sourceKey));
        return p.join(
                INNER,
                nested,
                aggregation,
                ImmutableList.of(new EquiJoinClause(factKey, sourceKey)),
                ImmutableList.of(quantity),
                Optional.of(p.rowExpression("quantity < average")));
    }
}
