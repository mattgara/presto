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
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.InMemoryExpressionOptimizerProvider;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.SystemSessionProperties.REWRITE_OUTER_JOIN_TO_INNER_THROUGH_AGGREGATION;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestOuterJoinToInnerThroughAggregation
        extends BasePlanTest
{
    // Orders with large line items: the left join keeps the other orders, whose groups the filter removes.
    private static final String JOIN = "FROM orders o LEFT JOIN lineitem l ON o.orderkey = l.orderkey AND l.quantity > 45 ";

    @Test
    public void testPositiveCountRewritesToInnerJoin()
    {
        assertRewritten("SELECT o.orderkey, count(l.orderkey), sum(l.partkey), max(l.shipdate) " + JOIN +
                "GROUP BY o.orderkey HAVING count(l.orderkey) > 0 ORDER BY 1");
        assertRewritten("SELECT o.orderkey, count(l.orderkey) " + JOIN +
                "GROUP BY o.orderkey HAVING count(l.orderkey) >= 1 ORDER BY 1");
    }

    @Test
    public void testNullRejectingAggregatesRewriteToInnerJoin()
    {
        // sum, min and max of the right side are NULL for a group of unmatched rows
        assertRewritten("SELECT o.custkey, sum(l.partkey) " + JOIN +
                "GROUP BY o.custkey HAVING sum(l.partkey) > 1000 ORDER BY 1");
        assertRewritten("SELECT o.orderkey, min(l.shipdate) " + JOIN +
                "GROUP BY o.orderkey HAVING min(l.shipdate) < DATE '1995-01-01' ORDER BY 1");
    }

    @Test
    public void testFilterKeepingUnmatchedGroupsKeepsLeftJoin()
    {
        // Groups of unmatched rows pass these filters, so the left join is needed
        assertKept("SELECT o.orderkey, count(l.orderkey) " + JOIN +
                "GROUP BY o.orderkey HAVING count(l.orderkey) = 0 ORDER BY 1");
        assertKept("SELECT o.orderkey, sum(l.partkey) " + JOIN +
                "GROUP BY o.orderkey HAVING coalesce(sum(l.partkey), 0) >= 0 ORDER BY 1");
        assertKept("SELECT o.orderkey, count(*) " + JOIN +
                "GROUP BY o.orderkey HAVING count(*) > 0 ORDER BY 1");
    }

    @Test
    public void testAggregatesSeeingUnmatchedRowsKeepLeftJoin()
    {
        // A customer group can hold matched and unmatched orders: count(*) and sum(o.totalprice) see the unmatched ones
        assertKept("SELECT o.custkey, count(l.orderkey), count(*) " + JOIN +
                "GROUP BY o.custkey HAVING count(l.orderkey) > 0 ORDER BY 1");
        assertKept("SELECT o.custkey, count(l.orderkey), sum(o.totalprice) " + JOIN +
                "GROUP BY o.custkey HAVING count(l.orderkey) > 0 ORDER BY 1");
        // coalesce(l.partkey, 0) is not NULL for an unmatched row
        assertKept("SELECT o.custkey, sum(coalesce(l.partkey, 0)) " + JOIN +
                "GROUP BY o.custkey HAVING sum(coalesce(l.partkey, 0)) > 0 ORDER BY 1");
    }

    @Test
    public void testUniqueIdBelowRepeatingJoinKeepsLeftJoin()
    {
        // The unique id is assigned below a join that repeats it: a group can hold matched and unmatched rows, and
        // sum(o2) sees the unmatched ones
        try (RuleTester tester = new RuleTester(ImmutableList.of(), ImmutableMap.of(REWRITE_OUTER_JOIN_TO_INNER_THROUGH_AGGREGATION, "true"))) {
            tester.assertThat(new PredicatePushDown(tester.getMetadata(), tester.getSqlParser(), new InMemoryExpressionOptimizerProvider(tester.getMetadata()), false))
                    .on(p -> {
                        VariableReferenceExpression o = p.variable("o");
                        VariableReferenceExpression o2 = p.variable("o2");
                        VariableReferenceExpression l = p.variable("l");
                        VariableReferenceExpression unique = p.variable("unique");
                        VariableReferenceExpression maximum = p.variable("m");
                        VariableReferenceExpression sum = p.variable("s");
                        return p.filter(
                                p.rowExpression("m > 0"),
                                p.aggregation(builder -> builder
                                        .source(p.join(
                                                JoinType.LEFT,
                                                p.join(JoinType.INNER, p.assignUniqueId(unique, p.values(o)), p.values(o2), new EquiJoinClause(o, o2)),
                                                p.values(l),
                                                new EquiJoinClause(o2, l)))
                                        .addAggregation(maximum, p.rowExpression("max(l)"))
                                        .addAggregation(sum, p.rowExpression("sum(o2)"))
                                        .singleGroupingSet(unique)));
                    })
                    .validates(plan -> assertEquals(countLeftJoins(plan.getRoot()), 1));
        }
    }

    @Test
    public void testLeavesAggregationPushableBelowJoin()
    {
        // A correlated scalar aggregate (TPC-H Q17 shape). PushAggregationThroughOuterJoin aggregates lineitem before the
        // join, as without the property, and the decorrelation's unique id is no longer needed.
        String sql = "SELECT o.orderkey FROM orders o WHERE o.totalprice < " +
                "(SELECT 1000 * avg(l.extendedprice) FROM lineitem l WHERE l.orderkey = o.orderkey) ORDER BY 1";
        assertEquals(countUniqueIds(sql, enabled()), 0, planText(sql, enabled()));
        assertEquals(countLeftJoins(sql, enabled()), 0, planText(sql, enabled()));
        assertEquals(getQueryRunner().execute(enabled(), sql), getQueryRunner().execute(sql));

        // Without that rule, the left join becomes an inner join below the aggregation
        Session withoutPushdown = Session.builder(enabled())
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "false")
                .build();
        assertEquals(countUniqueIds(sql, withoutPushdown), 1, planText(sql, withoutPushdown));
        assertEquals(countLeftJoins(sql, withoutPushdown), 0, planText(sql, withoutPushdown));
        assertEquals(getQueryRunner().execute(withoutPushdown, sql), getQueryRunner().execute(sql));
    }

    @Test
    public void testDisabledKeepsLeftJoin()
    {
        String sql = "SELECT o.orderkey, count(l.orderkey) " + JOIN + "GROUP BY o.orderkey HAVING count(l.orderkey) > 0 ORDER BY 1";
        assertEquals(countLeftJoins(sql, getQueryRunner().getDefaultSession()), 1);
    }

    private void assertRewritten(String sql)
    {
        assertEquals(countLeftJoins(sql, enabled()), 0, planText(sql, enabled()));
        assertEquals(countLeftJoins(sql, getQueryRunner().getDefaultSession()), 1);
        assertEquals(getQueryRunner().execute(enabled(), sql), getQueryRunner().execute(sql));
    }

    private void assertKept(String sql)
    {
        assertEquals(countLeftJoins(sql, enabled()), 1, planText(sql, enabled()));
        assertEquals(getQueryRunner().execute(enabled(), sql), getQueryRunner().execute(sql));
    }

    private Session enabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(REWRITE_OUTER_JOIN_TO_INNER_THROUGH_AGGREGATION, "true")
                .build();
    }

    private int countLeftJoins(String sql, Session session)
    {
        return countLeftJoins(plan(sql, OPTIMIZED, session).getRoot());
    }

    private static int countLeftJoins(PlanNode root)
    {
        return searchFrom(root)
                .where(node -> node instanceof JoinNode && ((JoinNode) node).getType() == JoinType.LEFT)
                .count();
    }

    private int countUniqueIds(String sql, Session session)
    {
        return searchFrom(plan(sql, OPTIMIZED, session).getRoot())
                .where(AssignUniqueId.class::isInstance)
                .count();
    }

    private String planText(String sql, Session session)
    {
        Plan plan = plan(sql, OPTIMIZED, session);
        return PlanPrinter.textLogicalPlan(plan.getRoot(), plan.getTypes(), StatsAndCosts.empty(), getMetadata().getFunctionAndTypeManager(), session, 0);
    }
}
