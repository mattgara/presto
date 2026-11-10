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

import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static java.lang.String.join;

/**
 * Presto row expressions as DuckDB SQL over columns named after the expressions' variables. Covers what scan
 * filters and projections over one table use (comparisons, arithmetic, boolean logic, IN, LIKE, CAST, NULL tests and
 * common scalar functions); anything else throws UnsupportedOperationException.
 */
final class ExpressionSql
{
    private static final Map<String, String> BINARY = Map.ofEntries(
            Map.entry("EQUAL", "="),
            Map.entry("NOT_EQUAL", "<>"),
            Map.entry("LESS_THAN", "<"),
            Map.entry("LESS_THAN_OR_EQUAL", "<="),
            Map.entry("GREATER_THAN", ">"),
            Map.entry("GREATER_THAN_OR_EQUAL", ">="),
            Map.entry("ADD", "+"),
            Map.entry("SUBTRACT", "-"),
            Map.entry("MULTIPLY", "*"),
            Map.entry("MODULUS", "%"),
            Map.entry("IS_DISTINCT_FROM", "IS DISTINCT FROM"));
    // Scalar functions with the same name and argument order in DuckDB.
    private static final Map<String, String> OPERATOR_SYMBOLS = Map.ofEntries(
            Map.entry("=", "EQUAL"),
            Map.entry("<>", "NOT_EQUAL"),
            Map.entry("!=", "NOT_EQUAL"),
            Map.entry("<", "LESS_THAN"),
            Map.entry("<=", "LESS_THAN_OR_EQUAL"),
            Map.entry(">", "GREATER_THAN"),
            Map.entry(">=", "GREATER_THAN_OR_EQUAL"),
            Map.entry("+", "ADD"),
            Map.entry("*", "MULTIPLY"),
            Map.entry("/", "DIVIDE"),
            Map.entry("%", "MODULUS"));
    private static final Set<String> FUNCTIONS = Set.of(
            "SUBSTR", "SUBSTRING", "LENGTH", "LOWER", "UPPER", "TRIM", "LTRIM", "RTRIM", "CONCAT", "STRPOS", "REPLACE",
            "ABS", "ROUND", "FLOOR", "CEIL", "CEILING", "YEAR", "MONTH", "DAY", "COALESCE");

    // Variables whose values are unknown (untranslated projections).
    private final Set<String> opaque;

    ExpressionSql(Set<String> opaque)
    {
        this.opaque = opaque;
    }

    String sql(RowExpression expression)
    {
        if (expression instanceof VariableReferenceExpression) {
            String name = ((VariableReferenceExpression) expression).getName();
            if (opaque.contains(name)) {
                throw new UnsupportedOperationException("untranslated projection " + name);
            }
            return quote(name);
        }
        if (expression instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) expression;
            return literal(constant.getType(), constant.getValue());
        }
        if (expression instanceof CallExpression) {
            return call((CallExpression) expression);
        }
        if (expression instanceof SpecialFormExpression) {
            return specialForm((SpecialFormExpression) expression);
        }
        throw new UnsupportedOperationException(expression.getClass().getSimpleName());
    }

    private String call(CallExpression call)
    {
        List<RowExpression> arguments = call.getArguments();
        String name = operatorName(call.getDisplayName().replace("$operator$", "").toUpperCase(Locale.ROOT), arguments.size());
        if (BINARY.containsKey(name) && arguments.size() == 2) {
            return format("(%s %s %s)", sql(arguments.get(0)), BINARY.get(name), sql(arguments.get(1)));
        }
        switch (name) {
            case "DIVIDE": {
                // Presto divides integers with truncation; DuckDB's '/' always returns a fraction.
                String operator = isIntegral(call.getType()) ? "//" : "/";
                return format("(%s %s %s)", sql(arguments.get(0)), operator, sql(arguments.get(1)));
            }
            case "NEGATION":
                return format("(-%s)", sql(arguments.get(0)));
            case "NOT":
                return format("(NOT %s)", sql(arguments.get(0)));
            case "BETWEEN":
                return format("(%s BETWEEN %s AND %s)", sql(arguments.get(0)), sql(arguments.get(1)), sql(arguments.get(2)));
            case "CAST":
                return format("CAST(%s AS %s)", sql(arguments.get(0)), duckdbType(call.getType()));
            case "LIKE":
                return format("(%s LIKE %s)", sql(arguments.get(0)), likePattern(arguments.get(1)));
            default:
                if (FUNCTIONS.contains(name)) {
                    List<String> sqlArguments = new ArrayList<>();
                    for (RowExpression argument : arguments) {
                        sqlArguments.add(sql(argument));
                    }
                    return format("%s(%s)", name.toLowerCase(Locale.ROOT), join(", ", sqlArguments));
                }
                throw new UnsupportedOperationException("function " + call.getDisplayName());
        }
    }

    // The pattern argument of LIKE: a VARCHAR constant, possibly cast to LikePattern. ESCAPE is not supported.
    // Planner rules name some operator calls by their symbol (for example "<>" in the strpos form of LIKE '%x%').
    private static String operatorName(String name, int argumentCount)
    {
        if (name.equals("-")) {
            return argumentCount == 1 ? "NEGATION" : "SUBTRACT";
        }
        return OPERATOR_SYMBOLS.getOrDefault(name, name);
    }

    private String likePattern(RowExpression pattern)
    {
        if (pattern instanceof CallExpression && ((CallExpression) pattern).getDisplayName().toUpperCase(Locale.ROOT).contains("CAST")) {
            pattern = ((CallExpression) pattern).getArguments().get(0);
        }
        if (pattern instanceof ConstantExpression && isString(((ConstantExpression) pattern).getType())) {
            return sql(pattern);
        }
        throw new UnsupportedOperationException("LIKE pattern " + pattern);
    }

    private String specialForm(SpecialFormExpression expression)
    {
        List<RowExpression> arguments = expression.getArguments();
        List<String> sqlArguments = new ArrayList<>();
        for (RowExpression argument : arguments) {
            sqlArguments.add(sql(argument));
        }
        switch (expression.getForm()) {
            case AND:
                return "(" + join(" AND ", sqlArguments) + ")";
            case OR:
                return "(" + join(" OR ", sqlArguments) + ")";
            case IN:
                return format("(%s IN (%s))", sqlArguments.get(0), join(", ", sqlArguments.subList(1, sqlArguments.size())));
            case IS_NULL:
                return format("(%s IS NULL)", sqlArguments.get(0));
            case COALESCE:
                return format("coalesce(%s)", join(", ", sqlArguments));
            case IF:
                return format("(CASE WHEN %s THEN %s ELSE %s END)", sqlArguments.get(0), sqlArguments.get(1),
                        sqlArguments.size() > 2 ? sqlArguments.get(2) : "NULL");
            case NULL_IF:
                return format("nullif(%s, %s)", sqlArguments.get(0), sqlArguments.get(1));
            default:
                throw new UnsupportedOperationException("special form " + expression.getForm());
        }
    }

    static String literal(Type type, Object value)
    {
        if (value == null) {
            return format("CAST(NULL AS %s)", duckdbType(type));
        }
        switch (type.getTypeSignature().getBase()) {
            case "boolean":
                return ((Boolean) value) ? "TRUE" : "FALSE";
            case "bigint":
            case "integer":
            case "smallint":
            case "tinyint":
                return value.toString();
            case "double":
                return format("CAST(%s AS DOUBLE)", value);
            case "real":
                return format("CAST(%s AS FLOAT)", Float.intBitsToFloat(((Long) value).intValue()));
            case "decimal": {
                DecimalType decimal = (DecimalType) type;
                BigInteger unscaled = value instanceof Long ? BigInteger.valueOf((Long) value) : Decimals.decodeUnscaledValue((Slice) value);
                return format("CAST('%s' AS DECIMAL(%d, %d))", new BigDecimal(unscaled, decimal.getScale()).toPlainString(),
                        decimal.getPrecision(), decimal.getScale());
            }
            case "varchar":
            case "char":
                return "'" + ((Slice) value).toStringUtf8().replace("'", "''") + "'";
            case "date":
                return format("DATE '%s'", LocalDate.ofEpochDay((Long) value));
            default:
                throw new UnsupportedOperationException("literal of type " + type);
        }
    }

    static String duckdbType(Type type)
    {
        switch (type.getTypeSignature().getBase()) {
            case "boolean":
                return "BOOLEAN";
            case "bigint":
                return "BIGINT";
            case "integer":
                return "INTEGER";
            case "smallint":
                return "SMALLINT";
            case "tinyint":
                return "TINYINT";
            case "double":
                return "DOUBLE";
            case "real":
                return "FLOAT";
            case "decimal":
                return format("DECIMAL(%d, %d)", ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale());
            case "varchar":
            case "char":
                return "VARCHAR";
            case "date":
                return "DATE";
            default:
                throw new UnsupportedOperationException("type " + type);
        }
    }

    static String quote(String identifier)
    {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static boolean isIntegral(Type type)
    {
        String base = type.getTypeSignature().getBase();
        return base.equals("bigint") || base.equals("integer") || base.equals("smallint") || base.equals("tinyint");
    }

    private static boolean isString(Type type)
    {
        String base = type.getTypeSignature().getBase();
        return base.equals("varchar") || base.equals("char");
    }
}
