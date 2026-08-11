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

import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestTransformCountFilteredLeftJoinToInnerJoin
        extends BaseRuleTest
{
    @Test
    public void testConvertsWhenRightCountMustBePositive()
    {
        tester().assertThat(rule())
                .on(p -> plan(p, "right_count > BIGINT '0'", "count(right_value)"))
                .matches(filter(
                        "right_count > BIGINT '0'",
                        aggregation(
                                ImmutableMap.of("right_count", functionCall("count", ImmutableList.of("right_value"))),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("left_key", "right_key")),
                                        Optional.empty(),
                                        values("left_key"),
                                        values("right_key", "right_value")))));
    }

    @Test
    public void testDoesNotConvertCountStar()
    {
        tester().assertThat(rule())
                .on(p -> plan(p, "right_count > BIGINT '0'", "count()"))
                .doesNotFire();
    }

    @Test
    public void testDoesNotConvertCountFromLeft()
    {
        tester().assertThat(rule())
                .on(p -> plan(p, "right_count > BIGINT '0'", "count(left_key)"))
                .doesNotFire();
    }

    @Test
    public void testDoesNotConvertWhenZeroCountIsSelected()
    {
        tester().assertThat(rule())
                .on(p -> plan(p, "right_count = BIGINT '0'", "count(right_value)"))
                .doesNotFire();
    }

    private TransformCountFilteredLeftJoinToInnerJoin rule()
    {
        return new TransformCountFilteredLeftJoinToInnerJoin(tester().getMetadata().getFunctionAndTypeManager());
    }

    private static PlanNode plan(
            com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder p,
            String filter,
            String aggregation)
    {
        VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
        VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
        VariableReferenceExpression rightValue = p.variable("right_value", BIGINT);
        VariableReferenceExpression rightCount = p.variable("right_count", BIGINT);

        PlanNode join = p.join(
                JoinType.LEFT,
                p.values(leftKey),
                p.values(rightKey, rightValue),
                new EquiJoinClause(leftKey, rightKey));
        PlanNode aggregate = p.aggregation(builder -> builder
                .source(join)
                .addAggregation(rightCount, p.rowExpression(aggregation))
                .singleGroupingSet(leftKey));
        return p.filter(p.rowExpression(filter), aggregate);
    }
}
