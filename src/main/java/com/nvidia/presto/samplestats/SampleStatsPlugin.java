/*
 * SPDX-FileCopyrightText: Copyright (c) 2026, NVIDIA CORPORATION.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.nvidia.presto.samplestats;

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;

/**
 * Plan statistics estimated from per-table samples instead of recorded history. The planner asks this provider for
 * the statistics of each plan node through Presto's history-based statistics interface; the provider evaluates the
 * nodes it can (filters and projections over one table) on that table's sample and scales the result.
 *
 * <p>Configuration: the properties file named by the environment variable {@code PRESTO_SAMPLE_STATS_CONFIG}
 * (default {@code /opt/sample-stats/sample-stats.properties}); without it the plugin provides nothing. Keys:
 * <ul>
 * <li>{@code sample.dir}: directory of {@code tables.properties} and one Parquet file per table (default: the
 * properties file's directory)</li>
 * <li>{@code node.types}: plan node classes to answer (default {@code FilterNode,ProjectNode})</li>
 * <li>{@code log.file}: optional log of every estimate (SQL, sample rows, scaled rows, milliseconds)</li>
 * </ul>
 */
public class SampleStatsPlugin
        implements Plugin
{
    static final String CONFIG_ENVIRONMENT_VARIABLE = "PRESTO_SAMPLE_STATS_CONFIG";
    static final String DEFAULT_CONFIG = "/opt/sample-stats/sample-stats.properties";
    // Plain table scans are left to the cost model, which knows table row counts from ANALYZE: estimating a scan
    // with its pushed-down filter applied gives the planner a scan output it does not expect and changed join orders
    // (TPC-H Q5, Q10). Aggregations are not needed for TPC-H's plans.
    static final String DEFAULT_NODE_TYPES = "FilterNode,ProjectNode";

    @Override
    public Iterable<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProviders()
    {
        Path config = Paths.get(Optional.ofNullable(System.getenv(CONFIG_ENVIRONMENT_VARIABLE)).orElse(DEFAULT_CONFIG));
        if (!Files.isRegularFile(config)) {
            return Collections.emptyList();
        }
        Properties properties = new Properties();
        try (InputStream in = Files.newInputStream(config)) {
            properties.load(in);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Path directory = config.toAbsolutePath().getParent();
        return Collections.singletonList(new SampleStatsProvider(
                Optional.ofNullable(properties.getProperty("sample.dir")).map(Paths::get).orElse(directory),
                Optional.ofNullable(properties.getProperty("log.file")).map(Paths::get).orElse(null),
                new HashSet<>(Arrays.asList(properties.getProperty("node.types", DEFAULT_NODE_TYPES).split(",")))));
    }
}
