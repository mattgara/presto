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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.EquatableValueSet;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.samplestats.SampleTable;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.samplestats.duckdb.ExpressionSql.literal;
import static com.facebook.presto.samplestats.duckdb.ExpressionSql.quote;
import static java.lang.String.format;
import static java.lang.String.join;

/**
 * Translates a plan subtree that reads one sampled table into SQL over the sample: a table scan (with the filters
 * its layout carries) under any filters and projections, and aggregations that either have no grouping keys or
 * group on the table's sample key (their group count then scales like rows). Anything else is not translated.
 */
final class PlanTranslator
{
    private final String schema;
    private final Map<String, SampleTable> tables;

    PlanTranslator(String schema, Map<String, SampleTable> tables)
    {
        this.schema = schema;
        this.tables = tables;
    }

    Optional<Relation> translate(PlanNode node)
    {
        if (node instanceof TableScanNode) {
            return scan((TableScanNode) node);
        }
        if (node instanceof FilterNode) {
            return filter((FilterNode) node);
        }
        if (node instanceof ProjectNode) {
            return project((ProjectNode) node);
        }
        if (node instanceof AggregationNode) {
            return aggregate((AggregationNode) node);
        }
        return Optional.empty();
    }

    private Optional<Relation> scan(TableScanNode node)
    {
        TableHandle handle = node.getTable();
        Optional<Map<?, ?>> identifier = handle.getLayout().map(PlanTranslator::identifier);
        SchemaTableName name = identifier.map(map -> (SchemaTableName) map.get("schemaTableName"))
                .orElseGet(() -> (SchemaTableName) invoke(handle.getConnectorHandle(), "getSchemaTableName"));
        SampleTable table = tables.get(name.getTableName());
        if (table == null || !name.getSchemaName().equals(schema)) {
            return Optional.empty();
        }
        List<String> columns = new ArrayList<>();
        Set<String> keyVariables = new HashSet<>();
        for (Map.Entry<VariableReferenceExpression, ColumnHandle> assignment : node.getAssignments().entrySet()) {
            String column = (String) invoke(assignment.getValue(), "getName");
            String variable = assignment.getKey().getName();
            columns.add(quote(column) + " AS " + quote(variable));
            if (column.equals(table.getKey())) {
                keyVariables.add(variable);
            }
        }
        List<String> where = new ArrayList<>();
        if (identifier.isPresent()) {
            Object domain = identifier.get().get("domainPredicate");
            if (domain instanceof TupleDomain) {
                where.add(domainSql((TupleDomain<?>) domain));
            }
            Object remaining = identifier.get().get("remainingPredicate");
            if (remaining instanceof RowExpression && !isTrue((RowExpression) remaining)) {
                // The remaining predicate names columns as variables.
                where.add(new ExpressionSql(new HashSet<>()).sql((RowExpression) remaining));
            }
        }
        String sql = format("SELECT %s FROM %s%s", columns.isEmpty() ? "1 AS \"_\"" : join(", ", columns), quote(table.getName()),
                where.isEmpty() ? "" : " WHERE " + join(" AND ", where));
        return Optional.of(new Relation(sql, table, keyVariables, new HashSet<>(), false));
    }

    private Optional<Relation> filter(FilterNode node)
    {
        Optional<Relation> source = translate(node.getSource());
        if (!source.isPresent()) {
            return Optional.empty();
        }
        Relation s = source.get();
        String predicate = new ExpressionSql(s.getOpaque()).sql(node.getPredicate());
        return Optional.of(new Relation(format("SELECT * FROM (%s) t WHERE %s", s.getSql(), predicate),
                s.getTable(), s.getKeyVariables(), s.getOpaque(), s.isGlobal()));
    }

    private Optional<Relation> project(ProjectNode node)
    {
        Optional<Relation> source = translate(node.getSource());
        if (!source.isPresent()) {
            return Optional.empty();
        }
        Relation s = source.get();
        ExpressionSql expressions = new ExpressionSql(s.getOpaque());
        List<String> columns = new ArrayList<>();
        Set<String> keyVariables = new HashSet<>();
        Set<String> opaque = new HashSet<>();
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : node.getAssignments().getMap().entrySet()) {
            String variable = assignment.getKey().getName();
            RowExpression expression = assignment.getValue();
            String sql;
            try {
                sql = expressions.sql(expression);
            }
            catch (UnsupportedOperationException e) {
                // Rows do not depend on a projection; only predicates above may not use this one.
                sql = "NULL";
                opaque.add(variable);
            }
            if (expression instanceof VariableReferenceExpression && s.getKeyVariables().contains(((VariableReferenceExpression) expression).getName())) {
                keyVariables.add(variable);
            }
            columns.add(sql + " AS " + quote(variable));
        }
        return Optional.of(new Relation(format("SELECT %s FROM (%s) t", columns.isEmpty() ? "1 AS \"_\"" : join(", ", columns), s.getSql()),
                s.getTable(), keyVariables, opaque, s.isGlobal()));
    }

    private Optional<Relation> aggregate(AggregationNode node)
    {
        if (node.getStep() != AggregationNode.Step.SINGLE) {
            return Optional.empty();
        }
        Optional<Relation> source = translate(node.getSource());
        if (!source.isPresent()) {
            return Optional.empty();
        }
        Relation s = source.get();
        List<String> keys = new ArrayList<>();
        Set<String> keyVariables = new HashSet<>();
        for (VariableReferenceExpression key : node.getGroupingKeys()) {
            if (s.isOpaque(key.getName())) {
                return Optional.empty();
            }
            keys.add(quote(key.getName()));
            if (s.getKeyVariables().contains(key.getName())) {
                keyVariables.add(key.getName());
            }
        }
        // Groups on other columns do not scale like rows (a sample holds fewer of them, in no fixed proportion).
        if (!keys.isEmpty() && keyVariables.isEmpty()) {
            return Optional.empty();
        }
        ExpressionSql expressions = new ExpressionSql(s.getOpaque());
        List<String> columns = new ArrayList<>(keys);
        Set<String> opaque = new HashSet<>();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> aggregation : node.getAggregations().entrySet()) {
            String variable = aggregation.getKey().getName();
            String sql;
            try {
                sql = aggregationSql(aggregation.getValue(), expressions);
            }
            catch (UnsupportedOperationException e) {
                sql = "NULL";
                opaque.add(variable);
            }
            columns.add(sql + " AS " + quote(variable));
        }
        String sql = format("SELECT %s FROM (%s) t%s", columns.isEmpty() ? "1 AS \"_\"" : join(", ", columns), s.getSql(),
                keys.isEmpty() ? "" : " GROUP BY " + join(", ", keys));
        return Optional.of(new Relation(sql, s.getTable(), keyVariables, opaque, keys.isEmpty()));
    }

    private static String aggregationSql(AggregationNode.Aggregation aggregation, ExpressionSql expressions)
    {
        if (aggregation.getFilter().isPresent() || aggregation.getMask().isPresent() || aggregation.getOrderBy().isPresent()) {
            throw new UnsupportedOperationException("aggregation with a filter, mask or ordering");
        }
        CallExpression call = aggregation.getCall();
        String name = call.getDisplayName().toLowerCase();
        if (!name.equals("sum") && !name.equals("count") && !name.equals("avg") && !name.equals("min") && !name.equals("max")) {
            throw new UnsupportedOperationException("aggregation " + name);
        }
        List<String> arguments = new ArrayList<>();
        for (RowExpression argument : call.getArguments()) {
            arguments.add(expressions.sql(argument));
        }
        String distinct = aggregation.isDistinct() ? "DISTINCT " : "";
        return format("%s(%s%s)", name, distinct, arguments.isEmpty() ? "*" : join(", ", arguments));
    }

    private static Map<?, ?> identifier(ConnectorTableLayoutHandle layout)
    {
        Object identifier = layout.getIdentifier(Optional.empty(), PlanCanonicalizationStrategy.DEFAULT);
        if (!(identifier instanceof Map)) {
            throw new UnsupportedOperationException("table layout identifier " + identifier.getClass().getSimpleName());
        }
        return (Map<?, ?>) identifier;
    }

    // Connector classes (e.g. Hive's table and column handles) are not visible to plugins; their accessors are.
    private static Object invoke(Object target, String method)
    {
        try {
            return target.getClass().getMethod(method).invoke(target);
        }
        catch (ReflectiveOperationException e) {
            throw new UnsupportedOperationException(target.getClass().getSimpleName() + "." + method, e);
        }
    }

    private static boolean isTrue(RowExpression expression)
    {
        return expression instanceof ConstantExpression && Boolean.TRUE.equals(((ConstantExpression) expression).getValue());
    }

    private static String domainSql(TupleDomain<?> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return "FALSE";
        }
        List<String> conjuncts = new ArrayList<>();
        for (Map.Entry<?, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
            Object key = entry.getKey();
            String column = quote(key instanceof Subfield ? ((Subfield) key).getRootName() : key.toString());
            Domain domain = entry.getValue();
            if (domain.isAll()) {
                continue;
            }
            conjuncts.add(domainSql(column, domain));
        }
        return conjuncts.isEmpty() ? "TRUE" : join(" AND ", conjuncts);
    }

    private static String domainSql(String column, Domain domain)
    {
        ValueSet values = domain.getValues();
        Type type = domain.getType();
        String nulls = domain.isNullAllowed() ? column + " IS NULL" : null;
        String valuesSql;
        if (values.isNone()) {
            valuesSql = null;
        }
        else if (values.isAll()) {
            valuesSql = column + " IS NOT NULL";
        }
        else if (values instanceof SortedRangeSet) {
            List<String> ranges = new ArrayList<>();
            List<String> points = new ArrayList<>();
            for (Range range : ((SortedRangeSet) values).getOrderedRanges()) {
                if (range.isSingleValue()) {
                    points.add(literal(type, range.getSingleValue()));
                    continue;
                }
                List<String> bounds = new ArrayList<>();
                if (!range.isLowUnbounded()) {
                    bounds.add(column + (range.isLowInclusive() ? " >= " : " > ") + literal(type, range.getLowBoundedValue()));
                }
                if (!range.isHighUnbounded()) {
                    bounds.add(column + (range.isHighInclusive() ? " <= " : " < ") + literal(type, range.getHighBoundedValue()));
                }
                ranges.add(bounds.isEmpty() ? column + " IS NOT NULL" : "(" + join(" AND ", bounds) + ")");
            }
            if (!points.isEmpty()) {
                ranges.add(column + " IN (" + join(", ", points) + ")");
            }
            valuesSql = "(" + join(" OR ", ranges) + ")";
        }
        else if (values instanceof EquatableValueSet) {
            EquatableValueSet set = (EquatableValueSet) values;
            List<String> items = new ArrayList<>();
            for (Object value : set.getValues()) {
                items.add(literal(type, value));
            }
            valuesSql = column + (set.isWhiteList() ? " IN (" : " NOT IN (") + join(", ", items) + ")";
        }
        else {
            throw new UnsupportedOperationException("value set " + values.getClass().getSimpleName());
        }
        if (valuesSql == null) {
            return nulls == null ? "FALSE" : nulls;
        }
        return nulls == null ? valuesSql : "(" + valuesSql + " OR " + nulls + ")";
    }
}
