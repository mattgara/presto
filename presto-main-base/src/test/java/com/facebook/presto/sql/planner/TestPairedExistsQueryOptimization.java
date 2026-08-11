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

import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestPairedExistsQueryOptimization
        extends BasePlanTest
{
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

    @Test
    public void testProducesTwoScansAndEquivalentResult()
    {
        Plan plan = plan(PAIRED_EXISTS_QUERY, OPTIMIZED);
        assertEquals(
                searchFrom(plan.getRoot())
                        .where(TableScanNode.class::isInstance)
                        .count(),
                2);
        assertEquals(
                searchFrom(plan.getRoot())
                        .where(node -> node instanceof JoinNode && ((JoinNode) node).getType() != JoinType.INNER)
                        .count(),
                0);
        assertEquals(
                getQueryRunner().execute(PAIRED_EXISTS_QUERY),
                getQueryRunner().execute(EXPLICIT_MERGED_QUERY));
    }
}
