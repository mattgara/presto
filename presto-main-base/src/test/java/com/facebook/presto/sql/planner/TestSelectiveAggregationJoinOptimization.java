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
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestSelectiveAggregationJoinOptimization
        extends BasePlanTest
{
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

    @Test
    public void testProducesDuplicateSafeSelectiveAggregationPlan()
    {
        // LocalTpchQueryRunner uses tiny tables whose statistics do not model the SF1000
        // selectivity that motivates this rule. Force only the cost threshold here; the
        // production gate is exercised with real SF1000 statistics by the benchmark.
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSH_AGGREGATION_BELOW_JOIN_BYTE_REDUCTION_THRESHOLD, "1000")
                .build();
        Plan plan = plan(CORRELATED_QUERY, OPTIMIZED, session);
        String planText = PlanPrinter.textLogicalPlan(
                plan.getRoot(),
                plan.getTypes(),
                StatsAndCosts.empty(),
                getMetadata().getFunctionAndTypeManager(),
                session,
                0);
        assertEquals(
                searchFrom(plan.getRoot())
                        .where(node -> node instanceof AssignUniqueId &&
                                ((AssignUniqueId) node).getIdVariable().getName().startsWith("join_row_id"))
                        .count(),
                1,
                planText);
        assertEquals(
                getQueryRunner().execute(CORRELATED_QUERY),
                getQueryRunner().execute(EXPLICIT_QUERY));
    }
}
