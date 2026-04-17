/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.common.minion;


/**
 * Overall freshness of a materialized view, derived from the individual
 * {@link PartitionState} values stored in {@link MvRuntimeMetadata#getPartitions()}.
 *
 * <p>This value is <b>pre-computed</b> by writers (Minion Executor, ConsistencyManager)
 * via {@link MvRuntimeMetadata#computeFreshness} and persisted in the runtime ZNode so
 * that the broker can read it cheaply without scanning all partition states.
 *
 * <h3>State semantics</h3>
 * <ul>
 *   <li>{@link #FRESH} — all materialized partitions are {@link PartitionState#VALID}.
 *       The MV data is fully consistent with the base table (within the materialized
 *       time range). Safe to use without reservation.</li>
 *   <li>{@link #STALE} — at least one partition is {@link PartitionState#STALE},
 *       meaning the base table data has been updated but the MV has not yet been
 *       refreshed. The MV returns <em>outdated</em> but not <em>incorrect</em> data.
 *       Acceptable for use cases that tolerate eventual consistency.</li>
 *   <li>{@link #DEGRADED} — at least one partition is {@link PartitionState#EXPIRED},
 *       meaning the base table data has been deleted while the MV still holds
 *       stale aggregation results. The MV may return <em>incorrect</em> data.
 *       Callers should consider skipping the MV or warning the user.</li>
 * </ul>
 *
 * <h3>Priority</h3>
 * {@code DEGRADED} takes precedence over {@code STALE}, which takes precedence
 * over {@code FRESH}.
 */
public enum MvFreshness {
  FRESH,
  STALE,
  DEGRADED
}
