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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.samplestats.FragmentEngine;
import com.facebook.presto.samplestats.FragmentResult;
import com.facebook.presto.samplestats.SampleSet;
import com.facebook.presto.samplestats.SampleTable;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableMap;
import org.duckdb.DuckDBDriver;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.samplestats.duckdb.ExpressionSql.quote;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Evaluates fragments in an in-process DuckDB database that holds the samples: the fragment is translated to SQL
 * over its table's sample (see {@link PlanTranslator}) and counted. Not thread safe; the provider calls it one
 * fragment at a time.
 */
public class DuckDbFragmentEngine
        implements FragmentEngine
{
    private final SampleSet samples;
    private final Connection connection;

    public DuckDbFragmentEngine(SampleSet samples)
    {
        this.samples = requireNonNull(samples, "samples is null");
        try {
            this.connection = new DuckDBDriver().connect("jdbc:duckdb:", new Properties());
            try (Statement statement = connection.createStatement()) {
                for (SampleTable table : samples.getTables().values()) {
                    statement.execute(format("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')",
                            quote(table.getName()), samples.getFile(table).toString().replace("'", "''")));
                }
            }
        }
        catch (SQLException e) {
            throw new IllegalStateException("Cannot load the samples of " + samples.getSchema(), e);
        }
    }

    @Override
    public Optional<FragmentResult> evaluate(PlanNode node)
    {
        Optional<Relation> relation = new PlanTranslator(samples.getSchema(), samples.getTables()).translate(node);
        if (!relation.isPresent()) {
            return Optional.empty();
        }
        Relation fragment = relation.get();
        List<VariableReferenceExpression> strings = new ArrayList<>();
        StringBuilder sql = new StringBuilder("SELECT count(*)");
        for (VariableReferenceExpression output : node.getOutputVariables()) {
            if (isString(output.getType()) && !fragment.isOpaque(output.getName())) {
                strings.add(output);
                sql.append(", avg(strlen(").append(quote(output.getName())).append("))");
            }
        }
        sql.append(" FROM (").append(fragment.getSql()).append(") t");
        try (Statement statement = connection.createStatement(); ResultSet rows = statement.executeQuery(sql.toString())) {
            rows.next();
            long count = rows.getLong(1);
            ImmutableMap.Builder<String, Double> lengths = ImmutableMap.builder();
            for (int i = 0; i < strings.size(); i++) {
                double length = rows.getDouble(i + 2);
                lengths.put(strings.get(i).getName(), rows.wasNull() ? 0 : length);
            }
            return Optional.of(new FragmentResult(fragment.getTable(), count, fragment.isGlobal(), lengths.build(), fragment.getSql()));
        }
        catch (SQLException e) {
            throw new RuntimeException(format("failed on the sample: %s | %s", e.getMessage(), fragment.getSql()), e);
        }
    }

    private static boolean isString(Type type)
    {
        String base = type.getTypeSignature().getBase();
        return base.equals("varchar") || base.equals("char");
    }
}
