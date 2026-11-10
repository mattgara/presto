/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.presto.samplestats;

import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import org.duckdb.DuckDBDriver;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Properties;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.nvidia.presto.samplestats.ExpressionSql.literal;
import static com.nvidia.presto.samplestats.Plans.call;
import static com.nvidia.presto.samplestats.Plans.constant;
import static com.nvidia.presto.samplestats.Plans.variable;
import static io.airlift.slice.Slices.utf8Slice;
import static org.testng.Assert.assertEquals;

public class TestExpressionSql
{
    private static final DecimalType QUANTITY = createDecimalType(15, 2);
    private static final VariableReferenceExpression NAME = variable("name", VARCHAR);
    private static final VariableReferenceExpression QTY = variable("qty", QUANTITY);
    private static final VariableReferenceExpression DAY = variable("day", DATE);
    private static final VariableReferenceExpression KEY = variable("key", BIGINT);

    private Connection connection;

    @BeforeClass
    public void setUp()
            throws SQLException
    {
        connection = new DuckDBDriver().connect("jdbc:duckdb:", new Properties());
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE t AS SELECT * FROM (VALUES " +
                    "('forest green lace', 5.00::DECIMAL(15, 2), DATE '1994-01-01', 1::BIGINT), " +
                    "('it''s green', 12.50::DECIMAL(15, 2), DATE '1995-06-30', 2::BIGINT), " +
                    "('misty rose', 30.00::DECIMAL(15, 2), DATE '1996-12-31', 3::BIGINT), " +
                    "('abc', 41.00::DECIMAL(15, 2), DATE '1997-01-01', NULL::BIGINT)" +
                    ") v(name, qty, day, key)");
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws SQLException
    {
        connection.close();
    }

    @Test
    public void testLiterals()
    {
        assertEquals(literal(BIGINT, 42L), "42");
        assertEquals(literal(QUANTITY, 30000L), "CAST('300.00' AS DECIMAL(15, 2))");
        assertEquals(literal(createDecimalType(38, 2), Decimals.encodeUnscaledValue(new BigInteger("12345678901234567890"))),
                "CAST('123456789012345678.90' AS DECIMAL(38, 2))");
        assertEquals(literal(VARCHAR, utf8Slice("it's")), "'it''s'");
        assertEquals(literal(DATE, 8766L), "DATE '1994-01-01'");
        assertEquals(literal(BOOLEAN, true), "TRUE");
        assertEquals(literal(BIGINT, null), "CAST(NULL AS BIGINT)");
    }

    @Test
    public void testPredicatesMatchPrestoSemantics()
            throws SQLException
    {
        assertEquals(count(call("LIKE", BOOLEAN, NAME, constant(utf8Slice("%green%"), VARCHAR))), 2);
        // Q9's pattern arrives as a cast to LikePattern in some plans.
        assertEquals(count(call("LIKE", BOOLEAN, NAME, call("CAST", VARCHAR, constant(utf8Slice("%GREEN%"), VARCHAR)))), 0);
        assertEquals(count(new SpecialFormExpression(AND, BOOLEAN,
                call("GREATER_THAN", BOOLEAN, QTY, constant(1000L, QUANTITY)),
                call("LESS_THAN", BOOLEAN, DAY, constant(9862L, DATE)))), 2);
        assertEquals(count(new SpecialFormExpression(IN, BOOLEAN,
                call("substr", VARCHAR, NAME, constant(1L, BIGINT), constant(2L, BIGINT)),
                constant(utf8Slice("fo"), VARCHAR), constant(utf8Slice("ab"), VARCHAR))), 2);
        assertEquals(count(call("not", BOOLEAN, new SpecialFormExpression(IS_NULL, BOOLEAN, KEY))), 3);
        assertEquals(count(call("BETWEEN", BOOLEAN, KEY, constant(2L, BIGINT), constant(3L, BIGINT))), 2);
        // Presto divides integers with truncation: 2 / 2 = 1 and 3 / 2 = 1.
        assertEquals(count(call("EQUAL", BOOLEAN, call("DIVIDE", BIGINT, KEY, constant(2L, BIGINT)), constant(1L, BIGINT))), 2);
        assertEquals(count(call("$operator$equal", BOOLEAN, KEY, constant(3L, BIGINT))), 1);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUnknownFunctionIsNotTranslated()
    {
        new ExpressionSql(Collections.emptySet()).sql(call("regexp_like", BOOLEAN, NAME, constant(utf8Slice("g.*"), VARCHAR)));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testUntranslatedProjectionIsNotUsed()
    {
        new ExpressionSql(Collections.singleton("name")).sql(call("EQUAL", BOOLEAN, NAME, constant(utf8Slice("abc"), VARCHAR)));
    }

    private long count(RowExpression predicate)
            throws SQLException
    {
        String sql = "SELECT count(*) FROM t WHERE " + new ExpressionSql(Collections.emptySet()).sql(predicate);
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql)) {
            rows.next();
            return rows.getLong(1);
        }
    }
}
