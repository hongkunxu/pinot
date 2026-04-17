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
package org.apache.pinot.common.metadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.minion.MvDefinitionMetadata;
import org.apache.pinot.common.minion.MvDefinitionMetadata.MvSplitSpec;
import org.apache.pinot.common.minion.MvFreshness;
import org.apache.pinot.common.minion.MvRuntimeMetadata;
import org.apache.pinot.common.minion.PartitionFingerprint;
import org.apache.pinot.common.minion.PartitionInfo;
import org.apache.pinot.common.minion.PartitionState;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class MaterializedViewMetadataTest {

  @Test
  public void testDefinitionRoundTrip() {
    String mvTableName = "mv_daily_order_amount_OFFLINE";
    Map<String, String> partitionExprMaps = new HashMap<>();
    partitionExprMaps.put("DaysSinceEpoch", "DaysSinceEpoch");

    MvSplitSpec splitSpec = new MvSplitSpec("ts", "1:MILLISECONDS:EPOCH", 86400000L);

    MvDefinitionMetadata original = new MvDefinitionMetadata(
        mvTableName,
        Arrays.asList("orders_OFFLINE", "products_OFFLINE"),
        "SELECT DaysSinceEpoch, city, count(*) as cnt FROM orders GROUP BY DaysSinceEpoch, city",
        partitionExprMaps,
        splitSpec);

    ZNRecord znRecord = original.toZNRecord();
    assertEquals(znRecord.getId(), mvTableName);

    MvDefinitionMetadata restored = MvDefinitionMetadata.fromZNRecord(znRecord);
    assertEquals(restored.getMvTableNameWithType(), mvTableName);
    assertEquals(restored.getBaseTables(), Arrays.asList("orders_OFFLINE", "products_OFFLINE"));
    assertNotNull(restored.getDefinedSql());
    assertEquals(restored.getPartitionExprMaps().size(), 1);
    assertEquals(restored.getPartitionExprMaps().get("DaysSinceEpoch"), "DaysSinceEpoch");

    MvSplitSpec restoredSpec = restored.getSplitSpec();
    assertNotNull(restoredSpec);
    assertEquals(restoredSpec.getSourceTimeColumn(), "ts");
    assertEquals(restoredSpec.getSourceTimeFormat(), "1:MILLISECONDS:EPOCH");
    assertEquals(restoredSpec.getBucketMs(), 86400000L);
  }

  @Test
  public void testDefinitionWithNoSplitSpec() {
    MvDefinitionMetadata metadata = new MvDefinitionMetadata(
        "mv_OFFLINE",
        Collections.singletonList("src_OFFLINE"),
        null,
        Collections.emptyMap(),
        null);

    ZNRecord znRecord = metadata.toZNRecord();
    MvDefinitionMetadata restored = MvDefinitionMetadata.fromZNRecord(znRecord);

    assertEquals(restored.getBaseTables(), Collections.singletonList("src_OFFLINE"));
    assertNull(restored.getDefinedSql());
    assertTrue(restored.getPartitionExprMaps().isEmpty());
    assertNull(restored.getSplitSpec());
  }

  @Test
  public void testRuntimeRoundTrip() {
    Map<Long, PartitionInfo> partitions = new HashMap<>();
    partitions.put(86400000L, new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(10, 5000L), 1700010000000L));
    partitions.put(172800000L, new PartitionInfo(
        PartitionState.STALE, new PartitionFingerprint(8, 3200L), 1700090000000L));
    partitions.put(259200000L, new PartitionInfo(
        PartitionState.EXPIRED, new PartitionFingerprint(0, 0L), 1700080000000L));

    MvRuntimeMetadata original = new MvRuntimeMetadata(
        "mv_test_OFFLINE", 259200000L, 259200000L, MvFreshness.DEGRADED, partitions);

    ZNRecord znRecord = original.toZNRecord();
    MvRuntimeMetadata restored = MvRuntimeMetadata.fromZNRecord(znRecord);

    assertEquals(restored.getMvTableNameWithType(), "mv_test_OFFLINE");
    assertEquals(restored.getWatermarkMs(), 259200000L);
    assertEquals(restored.getCoverageUpperMs(), 259200000L);
    assertEquals(restored.getFreshness(), MvFreshness.DEGRADED);
    assertEquals(restored.getPartitions().size(), 3);

    PartitionInfo info1 = restored.getPartitions().get(86400000L);
    assertEquals(info1.getState(), PartitionState.VALID);
    assertEquals(info1.getFingerprint(), new PartitionFingerprint(10, 5000L));
    assertEquals(info1.getLastRefreshTime(), 1700010000000L);

    assertEquals(restored.getPartitions().get(172800000L).getState(), PartitionState.STALE);
    assertEquals(restored.getPartitions().get(259200000L).getState(), PartitionState.EXPIRED);
  }

  @Test
  public void testRuntimeEmptyPartitions() {
    MvRuntimeMetadata metadata = new MvRuntimeMetadata(
        "mv_OFFLINE", 0L, 0L, MvFreshness.FRESH, new HashMap<>());

    ZNRecord znRecord = metadata.toZNRecord();
    MvRuntimeMetadata restored = MvRuntimeMetadata.fromZNRecord(znRecord);

    assertEquals(restored.getWatermarkMs(), 0L);
    assertEquals(restored.getCoverageUpperMs(), 0L);
    assertEquals(restored.getFreshness(), MvFreshness.FRESH);
    assertTrue(restored.getPartitions().isEmpty());
  }

  @Test
  public void testComputeFreshnessAllValid() {
    Map<Long, PartitionInfo> partitions = new HashMap<>();
    partitions.put(86400000L, new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(5, 1000L), 1700000000000L));
    partitions.put(172800000L, new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(3, 2000L), 1700000000000L));

    assertEquals(MvRuntimeMetadata.computeFreshness(partitions), MvFreshness.FRESH);
  }

  @Test
  public void testComputeFreshnessEmpty() {
    assertEquals(MvRuntimeMetadata.computeFreshness(new HashMap<>()), MvFreshness.FRESH);
  }

  @Test
  public void testComputeFreshnessWithStale() {
    Map<Long, PartitionInfo> partitions = new HashMap<>();
    partitions.put(86400000L, new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(5, 1000L), 1700000000000L));
    partitions.put(172800000L, new PartitionInfo(
        PartitionState.STALE, new PartitionFingerprint(3, 2000L), 1700000000000L));

    assertEquals(MvRuntimeMetadata.computeFreshness(partitions), MvFreshness.STALE);
  }

  @Test
  public void testComputeFreshnessExpiredTakesPrecedence() {
    Map<Long, PartitionInfo> partitions = new HashMap<>();
    partitions.put(86400000L, new PartitionInfo(
        PartitionState.STALE, new PartitionFingerprint(5, 1000L), 1700000000000L));
    partitions.put(172800000L, new PartitionInfo(
        PartitionState.EXPIRED, new PartitionFingerprint(0, 0L), 1700000000000L));
    partitions.put(259200000L, new PartitionInfo(
        PartitionState.VALID, new PartitionFingerprint(2, 500L), 1700000000000L));

    assertEquals(MvRuntimeMetadata.computeFreshness(partitions), MvFreshness.DEGRADED);
  }

  @Test
  public void testColdStartWatermarkOnlyCoverageZero() {
    MvRuntimeMetadata metadata = new MvRuntimeMetadata(
        "mv_OFFLINE", 86400000L, 0L, MvFreshness.FRESH, new HashMap<>());

    ZNRecord znRecord = metadata.toZNRecord();
    MvRuntimeMetadata restored = MvRuntimeMetadata.fromZNRecord(znRecord);

    assertEquals(restored.getWatermarkMs(), 86400000L);
    assertEquals(restored.getCoverageUpperMs(), 0L);
    assertEquals(restored.getFreshness(), MvFreshness.FRESH);
  }

  @Test
  public void testLegacyZnRecordFallback() {
    // Simulate a pre-migration ZNode that only has upperExclusiveMs
    ZNRecord legacy = new ZNRecord("mv_legacy_OFFLINE");
    legacy.setLongField("upperExclusiveMs", 172800000L);
    legacy.setSimpleField("freshness", "FRESH");

    MvRuntimeMetadata restored = MvRuntimeMetadata.fromZNRecord(legacy);

    assertEquals(restored.getWatermarkMs(), 172800000L);
    assertEquals(restored.getCoverageUpperMs(), 172800000L);
    assertEquals(restored.getFreshness(), MvFreshness.FRESH);
  }

  @Test
  public void testWatermarkAndCoverageIndependent() {
    MvRuntimeMetadata metadata = new MvRuntimeMetadata(
        "mv_OFFLINE", 259200000L, 172800000L, MvFreshness.FRESH, new HashMap<>());

    ZNRecord znRecord = metadata.toZNRecord();
    MvRuntimeMetadata restored = MvRuntimeMetadata.fromZNRecord(znRecord);

    assertEquals(restored.getWatermarkMs(), 259200000L);
    assertEquals(restored.getCoverageUpperMs(), 172800000L);
  }
}
