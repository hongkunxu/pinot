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
  public void testRoundTripWithoutFingerprints() {
    MaterializedViewTaskMetadata metadata =
        new MaterializedViewTaskMetadata("myTable_OFFLINE", 1700006400000L);

    ZNRecord znRecord = metadata.toZNRecord();
    assertEquals(znRecord.getId(), "myTable_OFFLINE");
    assertEquals(znRecord.getLongField("watermarkMs", -1), 1700006400000L);
    assertNull(znRecord.getMapField("partitionFingerprints"));

    MaterializedViewTaskMetadata deserialized = MaterializedViewTaskMetadata.fromZNRecord(znRecord);
    assertEquals(deserialized.getTableNameWithType(), "myTable_OFFLINE");
    assertEquals(deserialized.getWatermarkMs(), 1700006400000L);
    assertTrue(deserialized.getPartitionFingerprints().isEmpty());
  }

  @Test
  public void testRoundTripWithFingerprints() {
    Map<Long, PartitionFingerprint> fingerprints = new HashMap<>();
    fingerprints.put(1700006400000L, new PartitionFingerprint(10, 5000L));
    fingerprints.put(1700092800000L, new PartitionFingerprint(8, 3200L));

    MaterializedViewTaskMetadata metadata =
        new MaterializedViewTaskMetadata("myTable_OFFLINE", 1700092800000L, fingerprints);

    ZNRecord znRecord = metadata.toZNRecord();

    Map<String, String> rawMap = znRecord.getMapField("partitionFingerprints");
    assertNotNull(rawMap);
    assertEquals(rawMap.size(), 2);
    assertEquals(rawMap.get("1700006400000"), "10,5000");
    assertEquals(rawMap.get("1700092800000"), "8,3200");

    MaterializedViewTaskMetadata deserialized = MaterializedViewTaskMetadata.fromZNRecord(znRecord);
    assertEquals(deserialized.getWatermarkMs(), 1700092800000L);
    assertEquals(deserialized.getPartitionFingerprints().size(), 2);
    assertEquals(deserialized.getPartitionFingerprints().get(1700006400000L),
        new PartitionFingerprint(10, 5000L));
    assertEquals(deserialized.getPartitionFingerprints().get(1700092800000L),
        new PartitionFingerprint(8, 3200L));
  }

  @Test
  public void testBackwardCompatibilityFromOldZNRecord() {
    ZNRecord oldRecord = new ZNRecord("legacyTable_OFFLINE");
    oldRecord.setLongField("watermarkMs", 1600000000000L);

    MaterializedViewTaskMetadata metadata = MaterializedViewTaskMetadata.fromZNRecord(oldRecord);
    assertEquals(metadata.getTableNameWithType(), "legacyTable_OFFLINE");
    assertEquals(metadata.getWatermarkMs(), 1600000000000L);
    assertTrue(metadata.getPartitionFingerprints().isEmpty());
  }

  @Test
  public void testFingerprintMapIsMutable() {
    MaterializedViewTaskMetadata metadata =
        new MaterializedViewTaskMetadata("table_OFFLINE", 0L);
    assertTrue(metadata.getPartitionFingerprints().isEmpty());

    metadata.getPartitionFingerprints().put(1000L, new PartitionFingerprint(1, 100L));
    assertEquals(metadata.getPartitionFingerprints().size(), 1);

    ZNRecord znRecord = metadata.toZNRecord();
    MaterializedViewTaskMetadata roundTrip = MaterializedViewTaskMetadata.fromZNRecord(znRecord);
    assertEquals(roundTrip.getPartitionFingerprints().get(1000L), new PartitionFingerprint(1, 100L));
  }

  @Test
  public void testMergeSimulatesExecutorBehavior() {
    // Simulate existing metadata with one partition fingerprint
    Map<Long, PartitionFingerprint> existing = new HashMap<>();
    existing.put(86400000L, new PartitionFingerprint(5, 1000L));
    MaterializedViewTaskMetadata oldMetadata =
        new MaterializedViewTaskMetadata("table_OFFLINE", 86400000L, existing);

    // Simulate new task bringing a second partition
    ZNRecord oldZnRecord = oldMetadata.toZNRecord();
    MaterializedViewTaskMetadata readBack = MaterializedViewTaskMetadata.fromZNRecord(oldZnRecord);

    Map<Long, PartitionFingerprint> merged = new HashMap<>(readBack.getPartitionFingerprints());
    merged.put(172800000L, new PartitionFingerprint(3, 500L));

    MaterializedViewTaskMetadata newMetadata =
        new MaterializedViewTaskMetadata("table_OFFLINE", 172800000L, merged);
    ZNRecord newZnRecord = newMetadata.toZNRecord();

    MaterializedViewTaskMetadata finalMetadata = MaterializedViewTaskMetadata.fromZNRecord(newZnRecord);
    assertEquals(finalMetadata.getWatermarkMs(), 172800000L);
    assertEquals(finalMetadata.getPartitionFingerprints().size(), 2);
    assertEquals(finalMetadata.getPartitionFingerprints().get(86400000L),
        new PartitionFingerprint(5, 1000L));
    assertEquals(finalMetadata.getPartitionFingerprints().get(172800000L),
        new PartitionFingerprint(3, 500L));
  }
}
