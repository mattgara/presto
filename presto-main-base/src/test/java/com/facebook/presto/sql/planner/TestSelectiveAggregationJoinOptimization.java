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
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.PUSH_SELECTIVE_JOIN_BELOW_AGGREGATION;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestSelectiveAggregationJoinOptimization
        extends BasePlanTest
{
    // TPC-H Q17
    private static final String CORRELATED_QUERY =
            "SELECT sum(l.extendedprice) / 7.0 " +
                    "FROM lineitem l JOIN part p ON p.partkey = l.partkey " +
                    "WHERE p.brand = 'Brand#23' " +
                    "AND p.container = 'MED BOX' " +
                    "AND l.quantity < (" +
                    "  SELECT 0.2 * avg(l2.quantity) " +
                    "  FROM lineitem l2 " +
                    "  WHERE l2.partkey = p.partkey" +
                    ")";

    private static final String EXPLICIT_QUERY =
            "WITH thresholds AS (" +
                    "  SELECT l2.partkey, 0.2 * avg(l2.quantity) AS avg_quantity " +
                    "  FROM lineitem l2 JOIN part p ON p.partkey = l2.partkey " +
                    "  WHERE p.brand = 'Brand#23' " +
                    "  AND p.container = 'MED BOX' " +
                    "  GROUP BY l2.partkey" +
                    ") " +
                    "SELECT sum(l.extendedprice) / 7.0 " +
                    "FROM lineitem l JOIN thresholds t ON l.partkey = t.partkey " +
                    "WHERE l.quantity < t.avg_quantity";

    // TPC-H Q20 shape: the selected partsupp rows move below the lineitem aggregation, and the
    // comparison with the aggregate stays above it.
    private static final String FILTERED_QUERY =
            "SELECT ps.partkey, ps.suppkey " +
                    "FROM partsupp ps " +
                    "WHERE ps.suppkey <= 10 " +
                    "AND ps.availqty > (" +
                    "  SELECT 0.5 * sum(l.quantity) " +
                    "  FROM lineitem l " +
                    "  WHERE l.partkey = ps.partkey " +
                    "  AND l.suppkey = ps.suppkey " +
                    "  AND l.shipdate >= DATE '1994-01-01' " +
                    "  AND l.shipdate < DATE '1995-01-01'" +
                    ") " +
                    "ORDER BY 1, 2";

    // Duplicate and null keys on the side that moves below the aggregation.
    private static final String DUPLICATE_KEYS_QUERY =
            "SELECT d.partkey, d.tag, a.line_count, a.max_quantity " +
                    "FROM (VALUES (BIGINT '1', 'a'), (BIGINT '1', 'a'), (BIGINT '2', 'b'), (CAST(NULL AS BIGINT), 'c')) d(partkey, tag) " +
                    "JOIN (" +
                    "  SELECT partkey, count(*) AS line_count, max(quantity) AS max_quantity " +
                    "  FROM lineitem " +
                    "  GROUP BY partkey" +
                    ") a ON d.partkey = a.partkey " +
                    "ORDER BY 1, 2";

    @Test
    public void testMovesSelectiveJoinBelowAggregation()
    {
        // LocalQueryRunner uses tiny tables whose statistics do not model the SF1000
        // selectivity that motivates this rule, so the threshold is raised here. With it, the
        // join that remains above the aggregation after the part join moves passes the gate
        // too, and lineitem moves below the aggregation as well.
        Session enabled = enabled("1000");
        assertMovedJoins(CORRELATED_QUERY, enabled, 2);
        assertMovedJoins(CORRELATED_QUERY, getQueryRunner().getDefaultSession(), 0);
        assertEquals(
                getQueryRunner().execute(enabled, CORRELATED_QUERY),
                getQueryRunner().execute(CORRELATED_QUERY));
        assertEquals(
                getQueryRunner().execute(enabled, CORRELATED_QUERY),
                getQueryRunner().execute(EXPLICIT_QUERY));
    }

    @Test
    public void testKeepsFilterOnAggregateAboveAggregation()
    {
        Session enabled = enabled("1000");
        assertMovedJoins(FILTERED_QUERY, enabled, 1);
        assertEquals(
                getQueryRunner().execute(enabled, FILTERED_QUERY),
                getQueryRunner().execute(FILTERED_QUERY));
    }

    @Test
    public void testKeepsDuplicateRowsApart()
    {
        Session enabled = enabled("1");
        assertMovedJoins(DUPLICATE_KEYS_QUERY, enabled, 1);
        assertEquals(
                getQueryRunner().execute(enabled, DUPLICATE_KEYS_QUERY),
                getQueryRunner().execute(DUPLICATE_KEYS_QUERY));
        assertEquals(getQueryRunner().execute(enabled, DUPLICATE_KEYS_QUERY).getRowCount(), 3);
    }

    private Session enabled(String threshold)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSH_SELECTIVE_JOIN_BELOW_AGGREGATION, "true")
                .setSystemProperty(PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD, threshold)
                .build();
    }

    // The rule assigns one unique id for each join it moves.
    private void assertMovedJoins(String sql, Session session, int expected)
    {
        Plan plan = plan(sql, OPTIMIZED, session);
        assertEquals(
                searchFrom(plan.getRoot())
                        .where(node -> node instanceof AssignUniqueId &&
                                ((AssignUniqueId) node).getIdVariable().getName().startsWith("join_row_id"))
                        .count(),
                expected,
                PlanPrinter.textLogicalPlan(
                        plan.getRoot(),
                        plan.getTypes(),
                        StatsAndCosts.empty(),
                        getMetadata().getFunctionAndTypeManager(),
                        session,
                        0));
    }
}
