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
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class MaterializedViewTaskMetadataTest {

  @Test
  public void testRoundTripWithoutPartitionInfos() {
    MaterializedViewTaskMetadata metadata =
        new MaterializedViewTaskMetadata("myTable_OFFLINE", 1700006400000L);

    ZNRecord znRecord = metadata.toZNRecord();
    assertEquals(znRecord.getId(), "myTable_OFFLINE");
    assertEquals(znRecord.getLongField("watermarkMs", -1), 1700006400000L);
    assertNull(znRecord.getMapField("partitionInfos"));

    MaterializedViewTaskMetadata deserialized = MaterializedViewTaskMetadata.fromZNRecord(znRecord);
    assertEquals(deserialized.getTableNameWithType(), "myTable_OFFLINE");
    assertEquals(deserialized.getWatermarkMs(), 1700006400000L);
    assertTrue(deserialized.getPartitionInfos().isEmpty());
  }

  @Test
  public void testRoundTripWithPartitionInfos() {
    Map<Long, PartitionInfo> infos = new HashMap<>();
    infos.put(1700006400000L, new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(10, 5000L), 1700010000000L));
    infos.put(1700092800000L, new PartitionInfo(
        PartitionState.STALE, new PartitionFingerprint(8, 3200L), 1700090000000L));

    MaterializedViewTaskMetadata metadata =
        new MaterializedViewTaskMetadata("myTable_OFFLINE", 1700092800000L, infos);

    ZNRecord znRecord = metadata.toZNRecord();
    Map<String, String> rawMap = znRecord.getMapField("partitionInfos");
    assertNotNull(rawMap);
    assertEquals(rawMap.size(), 2);

    MaterializedViewTaskMetadata deserialized = MaterializedViewTaskMetadata.fromZNRecord(znRecord);
    assertEquals(deserialized.getWatermarkMs(), 1700092800000L);
    assertEquals(deserialized.getPartitionInfos().size(), 2);

    PartitionInfo info1 = deserialized.getPartitionInfos().get(1700006400000L);
    assertEquals(info1.getState(), PartitionState.VALID);
    assertEquals(info1.getFingerprint(), new PartitionFingerprint(10, 5000L));
    assertEquals(info1.getLastRefreshMs(), 1700010000000L);

    PartitionInfo info2 = deserialized.getPartitionInfos().get(1700092800000L);
    assertEquals(info2.getState(), PartitionState.STALE);
    assertEquals(info2.getFingerprint(), new PartitionFingerprint(8, 3200L));
    assertEquals(info2.getLastRefreshMs(), 1700090000000L);
  }

  @Test
  public void testBackwardCompatibilityFromOldZNRecordNoFingerprints() {
    ZNRecord oldRecord = new ZNRecord("legacyTable_OFFLINE");
    oldRecord.setLongField("watermarkMs", 1600000000000L);

    MaterializedViewTaskMetadata metadata = MaterializedViewTaskMetadata.fromZNRecord(oldRecord);
    assertEquals(metadata.getTableNameWithType(), "legacyTable_OFFLINE");
    assertEquals(metadata.getWatermarkMs(), 1600000000000L);
    assertTrue(metadata.getPartitionInfos().isEmpty());
  }

  @Test
  public void testBackwardCompatibilityFromLegacyPartitionFingerprints() {
    ZNRecord legacyRecord = new ZNRecord("legacyMV_OFFLINE");
    legacyRecord.setLongField("watermarkMs", 86400000L);

    Map<String, String> legacyFpMap = new HashMap<>();
    legacyFpMap.put("86400000", "5,1000");
    legacyFpMap.put("172800000", "3,500");
    legacyRecord.setMapField("partitionFingerprints", legacyFpMap);

    MaterializedViewTaskMetadata metadata = MaterializedViewTaskMetadata.fromZNRecord(legacyRecord);
    assertEquals(metadata.getWatermarkMs(), 86400000L);
    assertEquals(metadata.getPartitionInfos().size(), 2);

    PartitionInfo info1 = metadata.getPartitionInfos().get(86400000L);
    assertNotNull(info1);
    assertEquals(info1.getState(), PartitionState.VALID);
    assertEquals(info1.getFingerprint(), new PartitionFingerprint(5, 1000L));
    assertEquals(info1.getLastRefreshMs(), 0L);

    PartitionInfo info2 = metadata.getPartitionInfos().get(172800000L);
    assertNotNull(info2);
    assertEquals(info2.getState(), PartitionState.VALID);
    assertEquals(info2.getFingerprint(), new PartitionFingerprint(3, 500L));
    assertEquals(info2.getLastRefreshMs(), 0L);
  }

  @Test
  public void testNewFormatTakesPriorityOverLegacy() {
    ZNRecord record = new ZNRecord("table_OFFLINE");
    record.setLongField("watermarkMs", 86400000L);

    Map<String, String> legacyFpMap = new HashMap<>();
    legacyFpMap.put("86400000", "5,1000");
    record.setMapField("partitionFingerprints", legacyFpMap);

    Map<String, String> newInfoMap = new HashMap<>();
    newInfoMap.put("86400000", "S,10,2000,1700010000000");
    record.setMapField("partitionInfos", newInfoMap);

    MaterializedViewTaskMetadata metadata = MaterializedViewTaskMetadata.fromZNRecord(record);
    assertEquals(metadata.getPartitionInfos().size(), 1);

    PartitionInfo info = metadata.getPartitionInfos().get(86400000L);
    assertEquals(info.getState(), PartitionState.STALE);
    assertEquals(info.getFingerprint(), new PartitionFingerprint(10, 2000L));
    assertEquals(info.getLastRefreshMs(), 1700010000000L);
  }

  @Test
  public void testPartitionInfoMapIsMutable() {
    MaterializedViewTaskMetadata metadata =
        new MaterializedViewTaskMetadata("table_OFFLINE", 0L);
    assertTrue(metadata.getPartitionInfos().isEmpty());

    PartitionInfo info = new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(1, 100L), 1000L);
    metadata.getPartitionInfos().put(1000L, info);
    assertEquals(metadata.getPartitionInfos().size(), 1);

    ZNRecord znRecord = metadata.toZNRecord();
    MaterializedViewTaskMetadata roundTrip = MaterializedViewTaskMetadata.fromZNRecord(znRecord);
    assertEquals(roundTrip.getPartitionInfos().get(1000L), info);
  }

  @Test
  public void testMergeSimulatesExecutorBehavior() {
    Map<Long, PartitionInfo> existing = new HashMap<>();
    existing.put(86400000L, new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(5, 1000L), 1700000000000L));
    MaterializedViewTaskMetadata oldMetadata =
        new MaterializedViewTaskMetadata("table_OFFLINE", 86400000L, existing);

    ZNRecord oldZnRecord = oldMetadata.toZNRecord();
    MaterializedViewTaskMetadata readBack = MaterializedViewTaskMetadata.fromZNRecord(oldZnRecord);

    Map<Long, PartitionInfo> merged = new HashMap<>(readBack.getPartitionInfos());
    merged.put(172800000L, new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(3, 500L), 1700100000000L));

    MaterializedViewTaskMetadata newMetadata =
        new MaterializedViewTaskMetadata("table_OFFLINE", 172800000L, merged);
    ZNRecord newZnRecord = newMetadata.toZNRecord();

    MaterializedViewTaskMetadata finalMetadata = MaterializedViewTaskMetadata.fromZNRecord(newZnRecord);
    assertEquals(finalMetadata.getWatermarkMs(), 172800000L);
    assertEquals(finalMetadata.getPartitionInfos().size(), 2);
    assertEquals(finalMetadata.getPartitionInfos().get(86400000L).getFingerprint(),
        new PartitionFingerprint(5, 1000L));
    assertEquals(finalMetadata.getPartitionInfos().get(172800000L).getFingerprint(),
        new PartitionFingerprint(3, 500L));
  }
}
