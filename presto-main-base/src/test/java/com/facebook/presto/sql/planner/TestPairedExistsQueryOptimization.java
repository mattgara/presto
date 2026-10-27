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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.COMBINE_PAIRED_EXISTS_SUBQUERIES;
import static com.facebook.presto.SystemSessionProperties.REWRITE_OUTER_JOIN_TO_INNER_THROUGH_AGGREGATION;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestPairedExistsQueryOptimization
        extends BasePlanTest
{
    // The subqueries of TPC-H Q21
    private static final String PAIRED_EXISTS_QUERY =
            "SELECT count(*) " +
                    "FROM lineitem l1 " +
                    "WHERE EXISTS (" +
                    "  SELECT * FROM lineitem l2 " +
                    "  WHERE l2.orderkey = l1.orderkey " +
                    "    AND l2.suppkey <> l1.suppkey" +
                    ") " +
                    "AND NOT EXISTS (" +
                    "  SELECT * FROM lineitem l3 " +
                    "  WHERE l3.orderkey = l1.orderkey " +
                    "    AND l3.suppkey <> l1.suppkey " +
                    "    AND l3.receiptdate > l3.commitdate" +
                    ")";

    private static final String EXPLICIT_MERGED_QUERY =
            "SELECT count(*) " +
                    "FROM (" +
                    "  SELECT l1.orderkey, l1.linenumber, l1.suppkey," +
                    "    max(IF(l2.suppkey <> l1.suppkey, 1, 0)) AS has_other," +
                    "    max(IF(l2.suppkey <> l1.suppkey AND l2.receiptdate > l2.commitdate, 1, 0)) AS has_other_late " +
                    "  FROM lineitem l1 " +
                    "  JOIN lineitem l2 ON l2.orderkey = l1.orderkey " +
                    "  GROUP BY l1.orderkey, l1.linenumber, l1.suppkey" +
                    ") checks " +
                    "WHERE has_other = 1 AND has_other_late = 0";

    // The added condition is null for the first line of each order.
    private static final String NULLABLE_REFINEMENT_QUERY =
            "SELECT count(*), sum(l1.orderkey) " +
                    "FROM lineitem l1 " +
                    "WHERE EXISTS (" +
                    "  SELECT * FROM lineitem l2 " +
                    "  WHERE l2.orderkey = l1.orderkey " +
                    "    AND l2.suppkey <> l1.suppkey" +
                    ") " +
                    "AND NOT EXISTS (" +
                    "  SELECT * FROM lineitem l3 " +
                    "  WHERE l3.orderkey = l1.orderkey " +
                    "    AND l3.suppkey <> l1.suppkey " +
                    "    AND NULLIF(l3.linenumber, 1) > 2" +
                    ")";

    private static final String BOTH_EXISTS_QUERY =
            "SELECT count(*), sum(l1.orderkey) " +
                    "FROM lineitem l1 " +
                    "WHERE EXISTS (" +
                    "  SELECT * FROM lineitem l2 " +
                    "  WHERE l2.orderkey = l1.orderkey " +
                    "    AND l2.suppkey <> l1.suppkey" +
                    ") " +
                    "AND EXISTS (" +
                    "  SELECT * FROM lineitem l3 " +
                    "  WHERE l3.orderkey = l1.orderkey " +
                    "    AND l3.suppkey <> l1.suppkey " +
                    "    AND l3.receiptdate > l3.commitdate" +
                    ")";

    @Test
    public void testScansTableOnceForBothSubqueries()
    {
        Session enabled = session(COMBINE_PAIRED_EXISTS_SUBQUERIES);
        assertScans(PAIRED_EXISTS_QUERY, enabled, 2);
        assertScans(PAIRED_EXISTS_QUERY, getQueryRunner().getDefaultSession(), 3);
        assertEquals(
                getQueryRunner().execute(enabled, PAIRED_EXISTS_QUERY),
                getQueryRunner().execute(PAIRED_EXISTS_QUERY));
        assertEquals(
                getQueryRunner().execute(enabled, PAIRED_EXISTS_QUERY),
                getQueryRunner().execute(EXPLICIT_MERGED_QUERY));
    }

    @Test
    public void testNullRefinement()
    {
        Session enabled = session(COMBINE_PAIRED_EXISTS_SUBQUERIES);
        assertScans(NULLABLE_REFINEMENT_QUERY, enabled, 2);
        assertEquals(
                getQueryRunner().execute(enabled, NULLABLE_REFINEMENT_QUERY),
                getQueryRunner().execute(NULLABLE_REFINEMENT_QUERY));
    }

    @Test
    public void testBothExists()
    {
        Session enabled = session(COMBINE_PAIRED_EXISTS_SUBQUERIES);
        assertScans(BOTH_EXISTS_QUERY, enabled, 2);
        assertEquals(
                getQueryRunner().execute(enabled, BOTH_EXISTS_QUERY),
                getQueryRunner().execute(BOTH_EXISTS_QUERY));
    }

    @Test
    public void testPairedExistsWithPositiveCountRewriteHasOnlyInnerJoins()
    {
        // The combined aggregation is decorrelated into a left join that
        // rewrite_outer_join_to_inner_through_aggregation turns into an inner join.
        Session enabled = session(COMBINE_PAIRED_EXISTS_SUBQUERIES, REWRITE_OUTER_JOIN_TO_INNER_THROUGH_AGGREGATION);
        Plan plan = plan(PAIRED_EXISTS_QUERY, OPTIMIZED, enabled);
        assertEquals(
                searchFrom(plan.getRoot())
                        .where(node -> node instanceof JoinNode && ((JoinNode) node).getType() != JoinType.INNER)
                        .count(),
                0,
                planText(plan, enabled));
        assertEquals(
                getQueryRunner().execute(enabled, PAIRED_EXISTS_QUERY),
                getQueryRunner().execute(PAIRED_EXISTS_QUERY));
    }

    private Session session(String... enabledProperties)
    {
        Session.SessionBuilder session = Session.builder(getQueryRunner().getDefaultSession());
        for (String property : enabledProperties) {
            session.setSystemProperty(property, "true");
        }
        return session.build();
    }

    private void assertScans(String sql, Session session, int expected)
    {
        Plan plan = plan(sql, OPTIMIZED, session);
        assertEquals(
                searchFrom(plan.getRoot())
                        .where(TableScanNode.class::isInstance)
                        .count(),
                expected,
                planText(plan, session));
    }

    private String planText(Plan plan, Session session)
    {
        return PlanPrinter.textLogicalPlan(
                plan.getRoot(),
                plan.getTypes(),
                StatsAndCosts.empty(),
                getMetadata().getFunctionAndTypeManager(),
                session,
                0);
    }
}
