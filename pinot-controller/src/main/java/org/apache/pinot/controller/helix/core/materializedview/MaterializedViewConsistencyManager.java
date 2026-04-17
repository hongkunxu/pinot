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
package org.apache.pinot.controller.helix.core.materializedview;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.minion.MvDefinitionMetadata;
import org.apache.pinot.common.minion.MvFreshness;
import org.apache.pinot.common.minion.MvRuntimeMetadata;
import org.apache.pinot.common.minion.PartitionInfo;
import org.apache.pinot.common.minion.PartitionState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages MV partition consistency by reacting to base table segment changes.
 *
 * <p>When segments are added, replaced, or deleted in a base table, this manager
 * identifies which MV partitions overlap with the changed time range and marks
 * them as {@link PartitionState#STALE} in {@link MvRuntimeMetadata}.
 *
 * <p>Events are accumulated per base table using a debounce window ({@value DEBOUNCE_DELAY_MS}ms).
 * Multiple segment changes within the window are merged into a single time range and
 * processed as one ZK read-modify-write operation, avoiding excessive ZK traffic during
 * batch ingestion or bulk segment operations.
 *
 * <p>Thread-safety: all public methods are thread-safe. The internal flush runs on a
 * single-threaded scheduler to serialize ZK writes per base table.
 */
public class MaterializedViewConsistencyManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewConsistencyManager.class);

  private static final long DEBOUNCE_DELAY_MS = 5000;
  private static final int MAX_MARK_RETRIES = 3;

  private static final String BUCKET_TIME_PERIOD_KEY = "bucketTimePeriod";
  private static final String DEFAULT_BUCKET_PERIOD = "1d";

  /// Reverse index: rawBaseTableName → list of mvTableNameWithType.
  private final ConcurrentHashMap<String, List<String>> _baseTableToMvTables = new ConcurrentHashMap<>();

  /// Debounce buffer: rawBaseTableName → [minAffectedStartMs, maxAffectedEndMs].
  private final ConcurrentHashMap<String, long[]> _pendingRanges = new ConcurrentHashMap<>();

  /// Per-base-table scheduled flush future for debounce cancellation/reset.
  private final ConcurrentHashMap<String, ScheduledFuture<?>> _pendingTimers = new ConcurrentHashMap<>();

  private final ScheduledExecutorService _scheduler;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;

  public MaterializedViewConsistencyManager() {
    _scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "mv-consistency-manager");
      t.setDaemon(true);
      return t;
    });
  }

  /**
   * Initializes the manager by scanning existing MV definition metadata to build the reverse index.
   * Must be called after the PropertyStore is ready (after Controller startup).
   */
  public void init(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;
    rebuildReverseIndex();
    LOGGER.info("MaterializedViewConsistencyManager initialized with {} base table mappings",
        _baseTableToMvTables.size());
  }

  /**
   * Shuts down the scheduler. Pending flushes are discarded.
   */
  public void stop() {
    _scheduler.shutdownNow();
    _pendingRanges.clear();
    _pendingTimers.clear();
    LOGGER.info("MaterializedViewConsistencyManager stopped");
  }

  /**
   * Notifies that segments in a base table have changed. The affected time range is
   * accumulated in a debounce buffer; multiple calls within {@value DEBOUNCE_DELAY_MS}ms
   * for the same base table are merged into a single flush.
   *
   * <p>This method is O(1) and non-blocking (just a map merge + timer reset).
   *
   * @param baseTableName raw table name without type suffix
   * @param affectedStartMs earliest startTimeMs among changed segments
   * @param affectedEndMs   latest endTimeMs among changed segments
   */
  public void onBaseTableDataChange(String baseTableName, long affectedStartMs, long affectedEndMs) {
    if (!_baseTableToMvTables.containsKey(baseTableName)) {
      return;
    }
    if (affectedStartMs <= 0 || affectedEndMs <= 0) {
      LOGGER.debug("Skipping MV dirty marking for table {} with invalid time range [{}, {}]",
          baseTableName, affectedStartMs, affectedEndMs);
      return;
    }

    _pendingRanges.merge(baseTableName, new long[]{affectedStartMs, affectedEndMs},
        (existing, incoming) -> {
          existing[0] = Math.min(existing[0], incoming[0]);
          existing[1] = Math.max(existing[1], incoming[1]);
          return existing;
        });

    ScheduledFuture<?> oldTimer = _pendingTimers.put(baseTableName,
        _scheduler.schedule(() -> flush(baseTableName), DEBOUNCE_DELAY_MS, TimeUnit.MILLISECONDS));
    if (oldTimer != null) {
      oldTimer.cancel(false);
    }
  }

  /**
   * Updates the reverse index when a new MV table is created.
   */
  public void onMvTableCreated(String mvTableNameWithType, List<String> baseTables) {
    for (String baseTable : baseTables) {
      _baseTableToMvTables.computeIfAbsent(baseTable, k -> new CopyOnWriteArrayList<>())
          .add(mvTableNameWithType);
    }
    LOGGER.info("Registered MV table {} with base tables {}", mvTableNameWithType, baseTables);
  }

  /**
   * Updates the reverse index when an MV table is dropped.
   */
  public void onMvTableDropped(String mvTableNameWithType, List<String> baseTables) {
    for (String baseTable : baseTables) {
      List<String> mvTables = _baseTableToMvTables.get(baseTable);
      if (mvTables != null) {
        mvTables.remove(mvTableNameWithType);
        if (mvTables.isEmpty()) {
          _baseTableToMvTables.remove(baseTable);
        }
      }
    }
    LOGGER.info("Unregistered MV table {} from base tables {}", mvTableNameWithType, baseTables);
  }

  /**
   * Processes all accumulated changes for a base table in one batch.
   * For each affected MV, performs one ZK read + one ZK write on the runtime ZNode.
   */
  private void flush(String baseTableName) {
    long[] range = _pendingRanges.remove(baseTableName);
    _pendingTimers.remove(baseTableName);
    if (range == null) {
      return;
    }

    List<String> mvTables = _baseTableToMvTables.get(baseTableName);
    if (mvTables == null || mvTables.isEmpty()) {
      return;
    }

    LOGGER.info("Flushing MV dirty marking for base table: {}, affected range: [{}, {}], "
        + "affected MV tables: {}", baseTableName, range[0], range[1], mvTables);

    for (String mvTableName : mvTables) {
      markPartitionsDirtyWithRetry(mvTableName, range[0], range[1]);
    }
  }

  private void markPartitionsDirtyWithRetry(String mvTableName, long affectedStartMs, long affectedEndMs) {
    for (int attempt = 0; attempt < MAX_MARK_RETRIES; attempt++) {
      try {
        if (markPartitionsDirty(mvTableName, affectedStartMs, affectedEndMs)) {
          return;
        }
        LOGGER.warn("CAS conflict on attempt {} for MV table: {}, retrying...", attempt + 1, mvTableName);
      } catch (Exception e) {
        LOGGER.warn("Attempt {} failed to mark dirty for MV table: {}", attempt + 1, mvTableName, e);
      }
    }
    LOGGER.error("Failed to mark dirty partitions for MV table: {} after {} retries. "
        + "Generator verification will catch inconsistencies.", mvTableName, MAX_MARK_RETRIES);
  }

  /**
   * Marks overlapping VALID partitions as STALE in the MV runtime metadata.
   *
   * @return true if succeeded or nothing to mark; false if CAS failed (caller should retry)
   */
  private boolean markPartitionsDirty(String mvTableName, long affectedStartMs, long affectedEndMs) {
    String path = ZKMetadataProvider.constructPropertyStorePathForMvRuntime(mvTableName);
    Stat stat = new Stat();
    ZNRecord znRecord = _propertyStore.get(path, stat, AccessOption.PERSISTENT);
    if (znRecord == null) {
      return true;
    }

    MvRuntimeMetadata runtime = MvRuntimeMetadata.fromZNRecord(znRecord);
    Map<Long, PartitionInfo> partitionInfos = runtime.getPartitions();
    if (partitionInfos.isEmpty()) {
      return true;
    }

    long bucketMs = inferBucketMs(mvTableName, partitionInfos);
    if (bucketMs <= 0) {
      LOGGER.warn("Cannot infer bucket size for MV table: {}, skipping dirty marking", mvTableName);
      return true;
    }

    Map<Long, PartitionInfo> updatedInfos = new HashMap<>(partitionInfos);
    boolean anyChanged = false;
    int markedCount = 0;

    long partStart = (affectedStartMs / bucketMs) * bucketMs;
    while (partStart <= affectedEndMs) {
      PartitionInfo info = updatedInfos.get(partStart);
      if (info != null && info.getState() == PartitionState.VALID) {
        updatedInfos.put(partStart, info.withState(PartitionState.STALE));
        anyChanged = true;
        markedCount++;
      }
      partStart += bucketMs;
    }

    if (!anyChanged) {
      LOGGER.debug("No VALID partitions to mark STALE for MV table: {} in range [{}, {}]",
          mvTableName, affectedStartMs, affectedEndMs);
      return true;
    }

    MvFreshness freshness = MvRuntimeMetadata.computeFreshness(updatedInfos);
    MvRuntimeMetadata updated = new MvRuntimeMetadata(
        runtime.getMvTableNameWithType(), runtime.getWatermarkMs(),
        runtime.getCoverageUpperMs(), freshness, updatedInfos);

    boolean success = _propertyStore.set(path, updated.toZNRecord(), stat.getVersion(), AccessOption.PERSISTENT);
    if (success) {
      LOGGER.info("Marked {} partition(s) STALE for MV table: {} (range [{}, {}])",
          markedCount, mvTableName, affectedStartMs, affectedEndMs);
    }
    return success;
  }

  /**
   * Infers the bucket size in millis for the given MV table. First tries to read it from
   * the MV table's task config; falls back to computing the GCD of consecutive partition
   * start times from the partitionInfos map.
   */
  private long inferBucketMs(String mvTableName, Map<Long, PartitionInfo> partitionInfos) {
    long fromConfig = readBucketMsFromTableConfig(mvTableName);
    if (fromConfig > 0) {
      return fromConfig;
    }
    return inferBucketMsFromPartitions(partitionInfos);
  }

  private long readBucketMsFromTableConfig(String mvTableName) {
    try {
      String mvTableWithType = mvTableName.contains("_OFFLINE") ? mvTableName
          : TableNameBuilder.OFFLINE.tableNameWithType(mvTableName);
      TableConfig tableConfig = ZKMetadataProvider.getTableConfig(_propertyStore, mvTableWithType);
      if (tableConfig == null) {
        return -1;
      }
      TableTaskConfig taskConfig = tableConfig.getTaskConfig();
      if (taskConfig == null) {
        return -1;
      }
      Map<String, String> mvTaskConfigs =
          taskConfig.getConfigsForTaskType("MaterializedViewTask");
      if (mvTaskConfigs == null) {
        return -1;
      }
      String bucketPeriod = mvTaskConfigs.getOrDefault(BUCKET_TIME_PERIOD_KEY, DEFAULT_BUCKET_PERIOD);
      return TimeUtils.convertPeriodToMillis(bucketPeriod);
    } catch (Exception e) {
      LOGGER.debug("Failed to read bucket config for MV table: {}", mvTableName, e);
      return -1;
    }
  }

  /**
   * Computes bucket size by finding the minimum gap between sorted partition start times.
   */
  private long inferBucketMsFromPartitions(Map<Long, PartitionInfo> partitionInfos) {
    if (partitionInfos.size() < 2) {
      return -1;
    }
    List<Long> sortedKeys = new ArrayList<>(partitionInfos.keySet());
    sortedKeys.sort(Long::compareTo);

    long minGap = Long.MAX_VALUE;
    for (int i = 1; i < sortedKeys.size(); i++) {
      long gap = sortedKeys.get(i) - sortedKeys.get(i - 1);
      if (gap > 0) {
        minGap = Math.min(minGap, gap);
      }
    }
    return minGap == Long.MAX_VALUE ? -1 : minGap;
  }

  /**
   * Scans all MV definition ZNodes to build the baseTable → mvTable reverse index.
   */
  private void rebuildReverseIndex() {
    _baseTableToMvTables.clear();
    String defBasePath = ZKMetadataProvider.getPropertyStorePathForMvDefinitionPrefix();
    List<String> children = _propertyStore.getChildNames(defBasePath, AccessOption.PERSISTENT);
    if (children == null || children.isEmpty()) {
      LOGGER.info("No MV definition metadata found during reverse index rebuild");
      return;
    }

    for (String mvTableName : children) {
      try {
        String fullPath = defBasePath + "/" + mvTableName;
        ZNRecord record = _propertyStore.get(fullPath, null, AccessOption.PERSISTENT);
        if (record == null) {
          continue;
        }
        MvDefinitionMetadata definition = MvDefinitionMetadata.fromZNRecord(record);
        for (String baseTable : definition.getBaseTables()) {
          _baseTableToMvTables.computeIfAbsent(baseTable, k -> new CopyOnWriteArrayList<>())
              .add(mvTableName);
        }
      } catch (Exception e) {
        LOGGER.warn("Failed to load MV definition for: {}", mvTableName, e);
      }
    }
    LOGGER.info("Rebuilt MV reverse index: {}", _baseTableToMvTables);
  }
}
