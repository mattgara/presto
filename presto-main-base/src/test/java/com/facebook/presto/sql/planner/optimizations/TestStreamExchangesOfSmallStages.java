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
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.testing.TestingSession;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_MEMORY_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy.ALL;
import static com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy.AUTOMATIC;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestStreamExchangesOfSmallStages
        extends BasePlanTest
{
    // The join builds on customer (1,500 rows in the tiny schema, about 27 kB), the aggregation keeps five groups.
    private static final String JOIN_THEN_SMALL_AGGREGATION = "SELECT c.mktsegment, count(*) " +
            "FROM orders o JOIN customer c ON o.custkey = c.custkey " +
            "GROUP BY c.mktsegment";

    @Test
    public void testStreamsEveryExchangeWhenEveryStageFits()
    {
        assertEquals(exchanges(distributedPlan(JOIN_THEN_SMALL_AGGREGATION, session(ALL.name(), "1TB")), REMOTE_MATERIALIZED).size(), 3);
        assertEquals(exchanges(distributedPlan(JOIN_THEN_SMALL_AGGREGATION, session(AUTOMATIC.name(), "1TB")), REMOTE_MATERIALIZED).size(), 0);
    }

    @Test
    public void testMaterializesLikeAllWhenNoStageFits()
    {
        assertEquals(exchanges(distributedPlan(JOIN_THEN_SMALL_AGGREGATION, session(AUTOMATIC.name(), "1B")), REMOTE_MATERIALIZED).size(), 3);
    }

    @Test
    public void testMaterializesOnlyTheExchangesOfLargeStages()
    {
        Plan plan = distributedPlan(JOIN_THEN_SMALL_AGGREGATION, session(AUTOMATIC.name(), "5kB"));
        // The join's inputs are materialized.
        List<ExchangeNode> materialized = exchanges(plan, REMOTE_MATERIALIZED);
        assertEquals(materialized.size(), 2);
        materialized.forEach(exchange -> assertTrue(searchFrom(exchange).where(JoinNode.class::isInstance).findAll().isEmpty()));
        // The aggregation's input, which comes from the join, streams.
        List<ExchangeNode> streamed = exchanges(plan, REMOTE_STREAMING);
        assertEquals(streamed.stream().filter(exchange -> !searchFrom(exchange).where(JoinNode.class::isInstance).findAll().isEmpty()).count(), 1);
    }

    @Test
    public void testMaterializesWhenTheStageEstimateIsUnknown()
    {
        // Window functions have no statistics, so the join's build side has no size estimate. The window's own
        // stage, which holds customer, streams its input.
        String sql = "SELECT sum(c.total) FROM orders o " +
                "JOIN (SELECT custkey, sum(acctbal) OVER (PARTITION BY nationkey) total FROM customer) c ON o.custkey = c.custkey";
        assertEquals(exchanges(distributedPlan(sql, session(AUTOMATIC.name(), "1TB")), REMOTE_MATERIALIZED).size(), 2);
    }

    private Plan distributedPlan(String sql, Session session)
    {
        return plan(sql, OPTIMIZED_AND_VALIDATED, false, session);
    }

    private static List<ExchangeNode> exchanges(Plan plan, ExchangeNode.Scope scope)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getType() == REPARTITION && ((ExchangeNode) node).getScope() == scope)
                .findAll();
    }

    private static Session session(String strategy, String threshold)
    {
        return TestingSession.testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.toString())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.toString())
                .setSystemProperty(EXCHANGE_MATERIALIZATION_STRATEGY, strategy)
                .setSystemProperty(EXCHANGE_MATERIALIZATION_MEMORY_THRESHOLD, threshold)
                .build();
    }
}
