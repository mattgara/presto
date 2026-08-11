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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ExistsExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestTransformPairedExistsApplyToLateralNode
        extends BaseRuleTest
{
    @Test
    public void testCombinesRefinedPredicate()
    {
        tester().assertThat(rule())
                .on(p -> pairedApplyPlan(p, false, false))
                .matches(node(
                        com.facebook.presto.sql.planner.plan.LateralJoinNode.class,
                        values("outer_order", "outer_supp", "outer_threshold"),
                        node(
                                com.facebook.presto.spi.plan.ProjectNode.class,
                                node(
                                        com.facebook.presto.spi.plan.AggregationNode.class,
                                        node(
                                                com.facebook.presto.spi.plan.ProjectNode.class,
                                                node(
                                                        com.facebook.presto.spi.plan.FilterNode.class,
                                                        node(TableScanNode.class)))))));
    }

    @Test
    public void testDoesNotCombineUnrelatedPredicates()
    {
        tester().assertThat(rule())
                .on(p -> pairedApplyPlan(p, true, false))
                .doesNotFire();
    }

    @Test
    public void testDoesNotCombineCorrelatedRefinement()
    {
        tester().assertThat(rule())
                .on(p -> pairedApplyPlan(p, false, true))
                .doesNotFire();
    }

    private TransformPairedExistsApplyToLateralNode rule()
    {
        return new TransformPairedExistsApplyToLateralNode(tester().getMetadata().getFunctionAndTypeManager());
    }

    private static PlanNode pairedApplyPlan(
            com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder p,
            boolean unrelatedPredicate,
            boolean correlatedRefinement)
    {
        VariableReferenceExpression outerOrder = p.variable("outer_order", BIGINT);
        VariableReferenceExpression outerSupp = p.variable("outer_supp", BIGINT);
        VariableReferenceExpression outerThreshold = p.variable("outer_threshold", BIGINT);
        VariableReferenceExpression broadOrder = p.variable("broad_order", BIGINT);
        VariableReferenceExpression broadSupp = p.variable("broad_supp", BIGINT);
        VariableReferenceExpression narrowOrder = p.variable("narrow_order", BIGINT);
        VariableReferenceExpression narrowSupp = p.variable("narrow_supp", BIGINT);
        VariableReferenceExpression narrowReceipt = p.variable("narrow_receipt", BIGINT);
        VariableReferenceExpression narrowCommit = p.variable("narrow_commit", BIGINT);
        VariableReferenceExpression broadExists = p.variable("broad_exists", BOOLEAN);
        VariableReferenceExpression narrowExists = p.variable("narrow_exists", BOOLEAN);

        ColumnHandle orderColumn = new TestingColumnHandle("orderkey");
        ColumnHandle suppColumn = new TestingColumnHandle("suppkey");
        ColumnHandle receiptColumn = new TestingColumnHandle("receiptdate");
        ColumnHandle commitColumn = new TestingColumnHandle("commitdate");
        TableHandle table = new TableHandle(
                new ConnectorId("testConnector"),
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());

        PlanNode broadSubquery = p.project(
                Assignments.of(),
                p.filter(
                        p.rowExpression("broad_order = outer_order AND broad_supp <> outer_supp"),
                        p.tableScan(
                                table,
                                ImmutableList.of(broadOrder, broadSupp),
                                ImmutableMap.of(broadOrder, orderColumn, broadSupp, suppColumn))));

        String refinement;
        if (unrelatedPredicate) {
            refinement = "narrow_order = outer_order AND narrow_receipt > narrow_commit";
        }
        else if (correlatedRefinement) {
            refinement = "narrow_order = outer_order AND narrow_supp <> outer_supp AND narrow_receipt > outer_threshold";
        }
        else {
            refinement = "narrow_order = outer_order AND narrow_supp <> outer_supp AND narrow_receipt > narrow_commit";
        }

        PlanNode narrowSubquery = p.project(
                Assignments.of(),
                p.filter(
                        p.rowExpression(refinement),
                        p.tableScan(
                                table,
                                ImmutableList.of(narrowOrder, narrowSupp, narrowReceipt, narrowCommit),
                                ImmutableMap.of(
                                        narrowOrder, orderColumn,
                                        narrowSupp, suppColumn,
                                        narrowReceipt, receiptColumn,
                                        narrowCommit, commitColumn))));

        PlanNode input = p.values(outerOrder, outerSupp, outerThreshold);
        PlanNode inner = p.apply(
                assignment(broadExists, new ExistsExpression(Optional.empty(), TRUE_CONSTANT)),
                ImmutableList.of(outerOrder, outerSupp, outerThreshold),
                input,
                broadSubquery);
        return p.apply(
                assignment(narrowExists, new ExistsExpression(Optional.empty(), TRUE_CONSTANT)),
                ImmutableList.of(outerOrder, outerSupp, outerThreshold),
                inner,
                narrowSubquery);
    }
}
