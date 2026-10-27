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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ExistsExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.COMBINE_PAIRED_EXISTS_SUBQUERIES;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;

public class TestTransformPairedExistsApplyToLateralNode
        extends BaseRuleTest
{
    private static final TableHandle TABLE = new TableHandle(
            new ConnectorId("testConnector"),
            new TestingTableHandle(),
            TestingTransactionHandle.create(),
            Optional.empty());
    private static final List<String> CORRELATION = ImmutableList.of("outer_order", "outer_supp", "outer_threshold");

    private static final String BROAD = "broad_order = outer_order AND broad_supp <> outer_supp";
    private static final String REFINED = "narrow_order = outer_order AND narrow_supp <> outer_supp AND narrow_receipt > narrow_commit";
    private static final PlanMatchPattern COMBINED = node(
            LateralJoinNode.class,
            values("outer_order", "outer_supp", "outer_threshold"),
            node(ProjectNode.class,
                    node(AggregationNode.class,
                            node(ProjectNode.class,
                                    node(FilterNode.class,
                                            node(TableScanNode.class))))));

    @Test
    public void testCombinesRefinedPredicate()
    {
        assertThatRule()
                .on(p -> pairedApplyPlan(p, REFINED))
                .matches(COMBINED);
    }

    @Test
    public void testCombinesWhenRefinedSubqueryComesFirst()
    {
        assertThatRule()
                .on(p -> pairedApplyPlan(p, REFINED, true, TABLE, CORRELATION))
                .matches(COMBINED);
    }

    @Test
    public void testDoesNotCombineUnrelatedPredicates()
    {
        assertThatRule()
                .on(p -> pairedApplyPlan(p, "narrow_order = outer_order AND narrow_receipt > narrow_commit"))
                .doesNotFire();
    }

    @Test
    public void testDoesNotCombineWhenRefinedPredicateDropsAConjunct()
    {
        // More conjuncts than the broad predicate, but not narrow_supp <> outer_supp.
        assertThatRule()
                .on(p -> pairedApplyPlan(p, "narrow_order = outer_order AND narrow_receipt > narrow_commit AND narrow_supp > narrow_commit"))
                .doesNotFire();
    }

    @Test
    public void testDoesNotCombineEqualPredicates()
    {
        assertThatRule()
                .on(p -> pairedApplyPlan(p, "narrow_order = outer_order AND narrow_supp <> outer_supp"))
                .doesNotFire();
    }

    @Test
    public void testDoesNotCombineCorrelatedRefinement()
    {
        assertThatRule()
                .on(p -> pairedApplyPlan(p, "narrow_order = outer_order AND narrow_supp <> outer_supp AND narrow_receipt > outer_threshold"))
                .doesNotFire();
    }

    @Test
    public void testDoesNotCombineNondeterministicRefinement()
    {
        assertThatRule()
                .on(p -> pairedApplyPlan(p, "narrow_order = outer_order AND narrow_supp <> outer_supp AND random() > DOUBLE '0.5'"))
                .doesNotFire();
    }

    @Test
    public void testDoesNotCombineDifferentTables()
    {
        TableHandle otherTable = new TableHandle(
                new ConnectorId("otherConnector"),
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        assertThatRule()
                .on(p -> pairedApplyPlan(p, REFINED, false, otherTable, CORRELATION))
                .doesNotFire();
    }

    @Test
    public void testDoesNotCombineDifferentCorrelation()
    {
        assertThatRule()
                .on(p -> pairedApplyPlan(p, REFINED, false, TABLE, ImmutableList.of("outer_order", "outer_supp")))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(rule())
                .on(p -> pairedApplyPlan(p, REFINED))
                .doesNotFire();
    }

    private RuleAssert assertThatRule()
    {
        return tester().assertThat(rule())
                .setSystemProperty(COMBINE_PAIRED_EXISTS_SUBQUERIES, "true");
    }

    private TransformPairedExistsApplyToLateralNode rule()
    {
        return new TransformPairedExistsApplyToLateralNode(tester().getMetadata().getFunctionAndTypeManager());
    }

    private static PlanNode pairedApplyPlan(PlanBuilder p, String refinedPredicate)
    {
        return pairedApplyPlan(p, refinedPredicate, false, TABLE, CORRELATION);
    }

    // Applies EXISTS (broad subquery), then EXISTS (refined subquery), or the reverse when refinedFirst.
    // The refined subquery scans refinedTable and the second application correlates on secondCorrelation.
    private static PlanNode pairedApplyPlan(
            PlanBuilder p,
            String refinedPredicate,
            boolean refinedFirst,
            TableHandle refinedTable,
            List<String> secondCorrelation)
    {
        List<VariableReferenceExpression> firstCorrelation = CORRELATION.stream()
                .map(name -> p.variable(name, BIGINT))
                .collect(ImmutableList.toImmutableList());
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

        PlanNode broadSubquery = p.project(
                Assignments.of(),
                p.filter(
                        p.rowExpression(BROAD),
                        p.tableScan(
                                TABLE,
                                ImmutableList.of(broadOrder, broadSupp),
                                ImmutableMap.of(broadOrder, orderColumn, broadSupp, suppColumn))));
        PlanNode narrowSubquery = p.project(
                Assignments.of(),
                p.filter(
                        p.rowExpression(refinedPredicate),
                        p.tableScan(
                                refinedTable,
                                ImmutableList.of(narrowOrder, narrowSupp, narrowReceipt, narrowCommit),
                                ImmutableMap.of(
                                        narrowOrder, orderColumn,
                                        narrowSupp, suppColumn,
                                        narrowReceipt, receiptColumn,
                                        narrowCommit, commitColumn))));

        PlanNode first = p.apply(
                assignment(refinedFirst ? narrowExists : broadExists, new ExistsExpression(Optional.empty(), TRUE_CONSTANT)),
                firstCorrelation,
                p.values(firstCorrelation.toArray(new VariableReferenceExpression[0])),
                refinedFirst ? narrowSubquery : broadSubquery);
        return p.apply(
                assignment(refinedFirst ? broadExists : narrowExists, new ExistsExpression(Optional.empty(), TRUE_CONSTANT)),
                secondCorrelation.stream()
                        .map(name -> p.variable(name, BIGINT))
                        .collect(ImmutableList.toImmutableList()),
                first,
                refinedFirst ? broadSubquery : narrowSubquery);
    }
}
