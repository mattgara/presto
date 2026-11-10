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
package com.facebook.presto.samplestats.duckdb;

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

/**
 * Plan pieces as the planner hands them to the provider, with stand-ins for connector classes: a Hive-like column
 * handle (public getName()) and table layout whose identifier carries the table name and pushed-down predicates.
 */
final class Plans
{
    private static final FunctionHandle FUNCTION = new FunctionHandle()
    {
        @Override
        public CatalogSchemaName getCatalogSchemaName()
        {
            return new CatalogSchemaName("presto", "default");
        }

        @Override
        public String getName()
        {
            return "test";
        }

        @Override
        public FunctionKind getKind()
        {
            return FunctionKind.SCALAR;
        }

        @Override
        public List<TypeSignature> getArgumentTypes()
        {
            return Collections.emptyList();
        }
    };

    private Plans() {}

    static VariableReferenceExpression variable(String name, Type type)
    {
        return new VariableReferenceExpression(Optional.empty(), name, type);
    }

    static CallExpression call(String displayName, Type returnType, RowExpression... arguments)
    {
        return new CallExpression(displayName, FUNCTION, returnType, Arrays.asList(arguments));
    }

    static ConstantExpression constant(Object value, Type type)
    {
        return new ConstantExpression(value, type);
    }

    /** A scan of schema.table reading 'columns' (name -> type, in order), with 'domain' pushed into the layout. */
    static TableScanNode scan(String schema, String table, TupleDomain<Subfield> domain, Map<String, Type> columns)
    {
        List<VariableReferenceExpression> outputs = new ArrayList<>();
        Map<VariableReferenceExpression, ColumnHandle> assignments = new HashMap<>();
        for (Map.Entry<String, Type> column : columns.entrySet()) {
            VariableReferenceExpression variable = variable(column.getKey(), column.getValue());
            outputs.add(variable);
            assignments.put(variable, new Column(column.getKey()));
        }
        Map<String, Object> identifier = new LinkedHashMap<>();
        identifier.put("schemaTableName", new SchemaTableName(schema, table));
        identifier.put("domainPredicate", domain);
        identifier.put("remainingPredicate", constant(true, BOOLEAN));
        ConnectorTableLayoutHandle layout = new ConnectorTableLayoutHandle()
        {
            @Override
            public Object getIdentifier(Optional<ConnectorSplit> split, PlanCanonicalizationStrategy strategy)
            {
                return identifier;
            }
        };
        TableHandle handle = new TableHandle(
                new ConnectorId("hive"),
                new ConnectorTableHandle() {},
                new ConnectorTransactionHandle() {},
                Optional.of(layout));
        return new TableScanNode(Optional.empty(), new PlanNodeId(table), handle, outputs, assignments);
    }

    /** A Hive-like column handle; the provider reads the name through getName(). */
    public static final class Column
            implements ColumnHandle
    {
        private final String name;

        Column(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return name;
        }
    }

    /** Stand-in for Presto's StatsEquivalentPlanNodeWithLimit (an engine class), matched by simple name. */
    public static final class StatsEquivalentPlanNodeWithLimit
            extends PlanNode
    {
        private final PlanNode plan;

        StatsEquivalentPlanNodeWithLimit(PlanNode plan)
        {
            super(Optional.empty(), new PlanNodeId("limit-" + plan.getId()), Optional.empty());
            this.plan = plan;
        }

        public PlanNode getPlan()
        {
            return plan;
        }

        @Override
        public List<PlanNode> getSources()
        {
            return Collections.singletonList(plan);
        }

        @Override
        public List<VariableReferenceExpression> getOutputVariables()
        {
            return plan.getOutputVariables();
        }

        @Override
        public PlanNode replaceChildren(List<PlanNode> newChildren)
        {
            return new StatsEquivalentPlanNodeWithLimit(newChildren.get(0));
        }

        @Override
        public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
        {
            return this;
        }
    }
}
