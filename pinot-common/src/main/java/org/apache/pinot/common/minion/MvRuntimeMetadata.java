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

import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * Stores the mutable runtime state of a materialized view (MV): how far it has
 * been materialized, per-partition consistency info, and a pre-computed freshness
 * summary for the broker.
 *
 * <p>Persisted in ZooKeeper under {@code /CONFIGS/MV_RUNTIME/<mvTableNameWithType>}.
 * This ZNode is updated frequently by the Minion Executor (APPEND/OVERWRITE/DELETE),
 * the Controller ConsistencyManager (VALID→STALE marking), and the Generator
 * (STALE→VALID/EXPIRED state corrections).
 *
 * <h3>Watermark vs Coverage</h3>
 * <p>Two separate timestamps track different concerns:
 * <ul>
 *   <li>{@code watermarkMs} — <b>Generator only</b>: scheduling watermark indicating
 *       how far the generator has progressed. The next APPEND window starts here.
 *       Set during cold-start initialization and advanced by each successful APPEND.</li>
 *   <li>{@code coverageUpperMs} — <b>Broker only</b>: exclusive upper bound of
 *       confirmed queryable data. The MV covers {@code ts < coverageUpperMs}; the
 *       base-table complement covers {@code ts >= coverageUpperMs}. Only advanced
 *       after a successful APPEND writes actual data, so it remains {@code 0}
 *       during cold-start until the first APPEND completes.</li>
 * </ul>
 *
 * <p>This separation prevents the broker from issuing split queries against an
 * MV that has no data yet (cold-start scenario). Previously, a single
 * {@code upperExclusiveMs} served both purposes, which could cause silent data
 * loss if the broker saw a non-zero watermark before any data was appended.
 *
 * <p>Thread-safety: instances are effectively immutable after construction.
 */
public class MvRuntimeMetadata {

  private static final String UPPER_EXCLUSIVE_MS_KEY = "upperExclusiveMs";
  private static final String WATERMARK_MS_KEY = "watermarkMs";
  private static final String COVERAGE_UPPER_MS_KEY = "coverageUpperMs";
  private static final String FRESHNESS_KEY = "freshness";
  private static final String PARTITION_INFOS_MAP_KEY = "partitionInfos";

  private final String _mvTableNameWithType;
  private final long _watermarkMs;
  private final long _coverageUpperMs;
  private final MvFreshness _freshness;
  private final Map<Long, PartitionInfo> _partitions;

  public MvRuntimeMetadata(String mvTableNameWithType, long watermarkMs,
      long coverageUpperMs, MvFreshness freshness, Map<Long, PartitionInfo> partitions) {
    _mvTableNameWithType = mvTableNameWithType;
    _watermarkMs = watermarkMs;
    _coverageUpperMs = coverageUpperMs;
    _freshness = freshness;
    _partitions = partitions;
  }

  public String getMvTableNameWithType() {
    return _mvTableNameWithType;
  }

  public long getWatermarkMs() {
    return _watermarkMs;
  }

  public long getCoverageUpperMs() {
    return _coverageUpperMs;
  }

  /**
   * @deprecated Use {@link #getWatermarkMs()} for generator scheduling or
   *             {@link #getCoverageUpperMs()} for broker query coverage.
   */
  @Deprecated
  public long getUpperExclusiveMs() {
    return _watermarkMs;
  }

  public MvFreshness getFreshness() {
    return _freshness;
  }

  public Map<Long, PartitionInfo> getPartitions() {
    return _partitions;
  }

  /**
   * Computes the overall freshness from the partition states. Writers must call
   * this and persist the result into {@link #getFreshness() freshness} so that readers (broker)
   * can cheaply read the pre-computed value.
   */
  public static MvFreshness computeFreshness(Map<Long, PartitionInfo> partitions) {
    boolean hasStale = false;
    for (PartitionInfo info : partitions.values()) {
      if (info.getState() == PartitionState.EXPIRED) {
        return MvFreshness.DEGRADED;
      }
      if (info.getState() == PartitionState.STALE) {
        hasStale = true;
      }
    }
    return hasStale ? MvFreshness.STALE : MvFreshness.FRESH;
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_mvTableNameWithType);

    if (_watermarkMs > 0) {
      znRecord.setLongField(WATERMARK_MS_KEY, _watermarkMs);
      // Legacy field for mixed-version compatibility: old generators and brokers
      // read this field before the watermark/coverage split was introduced.
      znRecord.setLongField(UPPER_EXCLUSIVE_MS_KEY, _watermarkMs);
    }
    if (_coverageUpperMs > 0) {
      znRecord.setLongField(COVERAGE_UPPER_MS_KEY, _coverageUpperMs);
    }
    znRecord.setSimpleField(FRESHNESS_KEY, _freshness.name());

    if (!_partitions.isEmpty()) {
      Map<String, String> rawMap = new HashMap<>();
      for (Map.Entry<Long, PartitionInfo> entry : _partitions.entrySet()) {
        rawMap.put(Long.toString(entry.getKey()), entry.getValue().encode());
      }
      znRecord.setMapField(PARTITION_INFOS_MAP_KEY, rawMap);
    }

    return znRecord;
  }

  public static MvRuntimeMetadata fromZNRecord(ZNRecord znRecord) {
    String mvTableNameWithType = znRecord.getId();

    // Detect whether this is a new-format or legacy ZNode. New-format ZNodes
    // always have the WATERMARK_MS_KEY field; legacy ZNodes only have
    // UPPER_EXCLUSIVE_MS_KEY. For legacy ZNodes, both watermark and coverage
    // fall back to the single legacy value (correct for already-running MVs).
    // For new-format ZNodes, a missing coverageUpperMs means 0 (cold-start).
    long watermarkMs;
    long coverageUpperMs;
    if (znRecord.getSimpleFields().containsKey(WATERMARK_MS_KEY)) {
      watermarkMs = znRecord.getLongField(WATERMARK_MS_KEY, 0L);
      coverageUpperMs = znRecord.getLongField(COVERAGE_UPPER_MS_KEY, 0L);
    } else {
      long legacy = znRecord.getLongField(UPPER_EXCLUSIVE_MS_KEY, 0L);
      watermarkMs = legacy;
      coverageUpperMs = legacy;
    }

    String freshnessStr = znRecord.getSimpleField(FRESHNESS_KEY);
    MvFreshness freshness = freshnessStr != null
        ? MvFreshness.valueOf(freshnessStr) : MvFreshness.FRESH;

    Map<Long, PartitionInfo> partitions = new HashMap<>();
    Map<String, String> rawInfoMap = znRecord.getMapField(PARTITION_INFOS_MAP_KEY);
    if (rawInfoMap != null) {
      for (Map.Entry<String, String> entry : rawInfoMap.entrySet()) {
        long partitionStartMs = Long.parseLong(entry.getKey());
        partitions.put(partitionStartMs, PartitionInfo.decode(entry.getValue()));
      }
    }

    return new MvRuntimeMetadata(mvTableNameWithType, watermarkMs, coverageUpperMs,
        freshness, partitions);
  }
}
