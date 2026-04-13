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
 * Metadata for the minion task of type {@code MaterializedViewTask}.
 *
 * <p>Contains two kinds of state:
 * <ul>
 *   <li>{@code watermarkMs} – the time (exclusive) up to which tasks have been executed.</li>
 *   <li>{@code partitionInfos} – per-partition info (state, fingerprint, lastRefreshMs)
 *       recorded at materialization time, used to track partition freshness and detect
 *       base table data changes.</li>
 * </ul>
 *
 * <p>This gets serialized and stored in ZooKeeper under the path
 * MINION_TASK_METADATA/${tableNameWithType}/MaterializedViewTask
 *
 * <p>Backward compatibility: older ZNRecords that contain the legacy
 * {@code partitionFingerprints} mapField are automatically migrated to
 * {@link PartitionInfo} with {@code state=VALID, lastRefreshMs=0}.
 */
public class MaterializedViewTaskMetadata extends BaseTaskMetadata {

  private static final String WATERMARK_KEY = "watermarkMs";
  private static final String PARTITION_INFOS_MAP_KEY = "partitionInfos";
  private static final String LEGACY_PARTITION_FINGERPRINTS_MAP_KEY = "partitionFingerprints";

  private final String _tableNameWithType;
  private final long _watermarkMs;
  private final Map<Long, PartitionInfo> _partitionInfos;

  public MaterializedViewTaskMetadata(String tableNameWithType, long watermarkMs) {
    this(tableNameWithType, watermarkMs, new HashMap<>());
  }

  public MaterializedViewTaskMetadata(String tableNameWithType, long watermarkMs,
      Map<Long, PartitionInfo> partitionInfos) {
    _tableNameWithType = tableNameWithType;
    _watermarkMs = watermarkMs;
    _partitionInfos = partitionInfos;
  }

  @Override
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public long getWatermarkMs() {
    return _watermarkMs;
  }

  /**
   * Returns the mutable partition info map. Key is the partition start time in millis.
   */
  public Map<Long, PartitionInfo> getPartitionInfos() {
    return _partitionInfos;
  }

  public static MaterializedViewTaskMetadata fromZNRecord(ZNRecord znRecord) {
    long watermark = znRecord.getLongField(WATERMARK_KEY, 0);

    Map<Long, PartitionInfo> infos = new HashMap<>();

    // Try new format first
    Map<String, String> rawMap = znRecord.getMapField(PARTITION_INFOS_MAP_KEY);
    if (rawMap != null) {
      for (Map.Entry<String, String> entry : rawMap.entrySet()) {
        long partitionStartMs = Long.parseLong(entry.getKey());
        infos.put(partitionStartMs, PartitionInfo.decode(entry.getValue()));
      }
    } else {
      // Fall back to legacy format: convert PartitionFingerprint -> PartitionInfo(VALID, fp, 0)
      Map<String, String> legacyMap = znRecord.getMapField(LEGACY_PARTITION_FINGERPRINTS_MAP_KEY);
      if (legacyMap != null) {
        for (Map.Entry<String, String> entry : legacyMap.entrySet()) {
          long partitionStartMs = Long.parseLong(entry.getKey());
          PartitionFingerprint fp = PartitionFingerprint.decode(entry.getValue());
          infos.put(partitionStartMs, PartitionInfo.fromLegacyFingerprint(fp));
        }
      }
    }

    return new MaterializedViewTaskMetadata(znRecord.getId(), watermark, infos);
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setLongField(WATERMARK_KEY, _watermarkMs);

    if (!_partitionInfos.isEmpty()) {
      Map<String, String> rawMap = new HashMap<>();
      for (Map.Entry<Long, PartitionInfo> entry : _partitionInfos.entrySet()) {
        rawMap.put(Long.toString(entry.getKey()), entry.getValue().encode());
      }
      znRecord.setMapField(PARTITION_INFOS_MAP_KEY, rawMap);
    }

    return znRecord;
  }
}
