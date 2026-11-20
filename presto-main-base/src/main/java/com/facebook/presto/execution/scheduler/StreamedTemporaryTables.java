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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.ConnectorId;

import java.util.Map;

/**
 * Decides whether SqlQueryScheduler starts a plan section before its child sections finished (in place of
 * SystemSessionProperties.isEnhancedCTESchedulingEnabled at its two call sites).
 */
public final class StreamedTemporaryTables
{
    private StreamedTemporaryTables() {}

    // Enhanced CTE scheduling, or temporary tables read while they are written (the Hive session property
    // stream_temporary_tables, under which the Hive split manager lists a temporary table by its buckets before the
    // table exists): then the section reading a materialized exchange starts with the section writing it.
    public static boolean startSectionsEarly(Session session)
    {
        if (SystemSessionProperties.isEnhancedCTESchedulingEnabled(session)) {
            return true;
        }
        Map<String, String> hiveProperties = session.getConnectorProperties().get(new ConnectorId("hive"));
        return hiveProperties != null && Boolean.parseBoolean(hiveProperties.get("stream_temporary_tables"));
    }
}
