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
 *   <li>{@code partitionFingerprints} – per-partition fingerprint (segment count + CRC checksum)
 *       recorded at materialization time, used to detect base table data changes.</li>
 * </ul>
 *
 * <p>This gets serialized and stored in ZooKeeper under the path
 * MINION_TASK_METADATA/${tableNameWithType}/MaterializedViewTask
 *
 * <p>PinotTaskGenerator:
 * The {@code watermarkMs} is used by the {@code MaterializedViewTaskGenerator}
 * to determine the execution window: [watermarkMs, watermarkMs + bucketSize).
 *
 * <p>PinotTaskExecutor:
 * The same watermark is used by the {@code MaterializedViewTaskExecutor} to:
 * <ul>
 *   <li>Verify that it is running the latest task scheduled by the generator</li>
 *   <li>Update the watermark to the end of the window upon successful execution</li>
 *   <li>Persist the partition fingerprints for newly materialized partitions</li>
 * </ul>
 */
public class MaterializedViewTaskMetadata extends BaseTaskMetadata {

  private static final String WATERMARK_KEY = "watermarkMs";
  private static final String PARTITION_FINGERPRINTS_MAP_KEY = "partitionFingerprints";

  private final String _tableNameWithType;
  private final long _watermarkMs;
  private final Map<Long, PartitionFingerprint> _partitionFingerprints;

  public MaterializedViewTaskMetadata(String tableNameWithType, long watermarkMs) {
    this(tableNameWithType, watermarkMs, new HashMap<>());
  }

  public MaterializedViewTaskMetadata(String tableNameWithType, long watermarkMs,
      Map<Long, PartitionFingerprint> partitionFingerprints) {
    _tableNameWithType = tableNameWithType;
    _watermarkMs = watermarkMs;
    _partitionFingerprints = partitionFingerprints;
  }

  @Override
  public String getTableNameWithType() {
    return _tableNameWithType;
  }

  public long getWatermarkMs() {
    return _watermarkMs;
  }

  /**
   * Returns the mutable partition fingerprint map. Key is the partition start time in millis;
   * value is the fingerprint recorded when the partition was last materialized.
   */
  public Map<Long, PartitionFingerprint> getPartitionFingerprints() {
    return _partitionFingerprints;
  }

  public static MaterializedViewTaskMetadata fromZNRecord(ZNRecord znRecord) {
    long watermark = znRecord.getLongField(WATERMARK_KEY, 0);

    Map<Long, PartitionFingerprint> fingerprints = new HashMap<>();
    Map<String, String> rawMap = znRecord.getMapField(PARTITION_FINGERPRINTS_MAP_KEY);
    if (rawMap != null) {
      for (Map.Entry<String, String> entry : rawMap.entrySet()) {
        long partitionStartMs = Long.parseLong(entry.getKey());
        PartitionFingerprint fp = PartitionFingerprint.decode(entry.getValue());
        fingerprints.put(partitionStartMs, fp);
      }
    }

    return new MaterializedViewTaskMetadata(znRecord.getId(), watermark, fingerprints);
  }

  @Override
  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_tableNameWithType);
    znRecord.setLongField(WATERMARK_KEY, _watermarkMs);

    if (!_partitionFingerprints.isEmpty()) {
      Map<String, String> rawMap = new HashMap<>();
      for (Map.Entry<Long, PartitionFingerprint> entry : _partitionFingerprints.entrySet()) {
        rawMap.put(Long.toString(entry.getKey()), entry.getValue().encode());
      }
      znRecord.setMapField(PARTITION_FINGERPRINTS_MAP_KEY, rawMap);
    }

    return znRecord;
  }
}
