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
package com.facebook.presto.samplestats.plugin;

import com.facebook.presto.samplestats.FragmentEngineFactory;
import com.facebook.presto.samplestats.SampleSet;
import com.facebook.presto.samplestats.SampleStatsProvider;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Plan statistics from per-table samples, combined with the statistics Presto records from finished queries. The
 * planner asks this provider for the statistics of each plan node through Presto's history-based statistics
 * interface; see {@link SampleStatsProvider}.
 *
 * <p>Configuration: the properties file named by the environment variable {@code PRESTO_SAMPLE_STATS_CONFIG}
 * (default {@code /opt/presto-plugin-data/sample-stats.properties}); without it the plugin provides nothing. Keys:
 * <ul>
 * <li>{@code sample.dir}: directory of {@code tables.properties} and one Parquet file per table (default: the
 * properties file's directory)</li>
 * <li>{@code engine}: the fragment engine that evaluates fragments on the samples (default {@code duckdb})</li>
 * <li>{@code node.types}: plan node classes to estimate (default {@code FilterNode,ProjectNode})</li>
 * <li>{@code history.record}: keep the statistics Presto records from finished queries and prefer them over sample
 * estimates (default {@code true}; Presto records only with history tracking on)</li>
 * <li>{@code history.max-nodes}: the most plan nodes whose recorded statistics are kept (default 100000)</li>
 * <li>{@code log.file}: optional log of every estimate (fragment, sample rows, scaled rows, milliseconds)</li>
 * </ul>
 */
public class SampleStatsPlugin
        implements Plugin
{
    static final String CONFIG_ENVIRONMENT_VARIABLE = "PRESTO_SAMPLE_STATS_CONFIG";
    static final String DEFAULT_CONFIG = "/opt/presto-plugin-data/sample-stats.properties";
    // Plain table scans are left to the cost model, which knows table row counts from ANALYZE: estimating a scan
    // with its pushed-down filter applied gives the planner a scan output it does not expect and changed join orders
    // (TPC-H Q5, Q10). Aggregations are not needed for TPC-H's plans.
    static final String DEFAULT_NODE_TYPES = "FilterNode,ProjectNode";

    private final Optional<Path> config;

    public SampleStatsPlugin()
    {
        this(Optional.empty());
    }

    @VisibleForTesting
    SampleStatsPlugin(Optional<Path> config)
    {
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public Iterable<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProviders()
    {
        Path path = config.orElseGet(() -> Paths.get(Optional.ofNullable(System.getenv(CONFIG_ENVIRONMENT_VARIABLE)).orElse(DEFAULT_CONFIG)));
        if (!Files.isRegularFile(path)) {
            return ImmutableList.of();
        }
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            properties.load(in);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Path directory = path.toAbsolutePath().getParent();
        SampleSet samples = SampleSet.load(Optional.ofNullable(properties.getProperty("sample.dir")).map(Paths::get).orElse(directory));
        String engine = properties.getProperty("engine", "duckdb");
        FragmentEngineFactory factory = findEngine(engine);
        return ImmutableList.of(new SampleStatsProvider(
                factory.create(samples),
                ImmutableSet.copyOf(Splitter.on(',').trimResults().omitEmptyStrings().split(properties.getProperty("node.types", DEFAULT_NODE_TYPES))),
                Boolean.parseBoolean(properties.getProperty("history.record", "true")),
                Integer.parseInt(properties.getProperty("history.max-nodes", "100000")),
                Optional.ofNullable(properties.getProperty("log.file")).map(Paths::get)));
    }

    private static FragmentEngineFactory findEngine(String name)
    {
        for (FragmentEngineFactory factory : ServiceLoader.load(FragmentEngineFactory.class, SampleStatsPlugin.class.getClassLoader())) {
            if (factory.getName().equals(name)) {
                return factory;
            }
        }
        throw new IllegalArgumentException(format("No fragment engine named %s in the plugin directory", name));
    }
}
