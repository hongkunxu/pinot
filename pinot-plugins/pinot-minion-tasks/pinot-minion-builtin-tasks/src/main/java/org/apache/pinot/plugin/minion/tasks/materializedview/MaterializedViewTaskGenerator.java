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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MvDefinitionMetadata;
import org.apache.pinot.common.minion.MvDefinitionMetadataUtils;
import org.apache.pinot.common.minion.MvFreshness;
import org.apache.pinot.common.minion.MvRuntimeMetadata;
import org.apache.pinot.common.minion.MvRuntimeMetadataUtils;
import org.apache.pinot.common.minion.PartitionFingerprint;
import org.apache.pinot.common.minion.PartitionInfo;
import org.apache.pinot.common.minion.PartitionState;
import org.apache.pinot.controller.helix.core.minion.generator.BaseTaskGenerator;
import org.apache.pinot.controller.helix.core.minion.generator.TaskGeneratorUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.spi.annotations.minion.TaskGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task generator for {@link MaterializedViewTask}.
 *
 * <p>Unlike segment-conversion tasks, this generator does not scan source segments. It only
 * computes a time window and appends it to the user-defined SQL, producing a
 * {@link PinotTaskConfig} for the executor.
 *
 * <p>Two-step decision logic (evaluated per table, per schedule cycle):
 * <ol>
 *   <li><b>Overwrite STALE</b> – If any partition is marked {@link PartitionState#STALE}
 *       (by the event-driven {@code MaterializedViewConsistencyManager}), the generator
 *       performs a precise fingerprint verification. If the data truly changed, it generates
 *       an {@code OVERWRITE} task for the earliest stale partition. If the fingerprint
 *       matches (false positive), the partition is reverted to {@link PartitionState#VALID}.
 *       This step has the highest priority to maintain consistency.</li>
 *   <li><b>Append</b> – If no STALE partitions exist and the watermark can advance (next
 *       window is outside the buffer period), generate a normal {@code APPEND} task.</li>
 * </ol>
 *
 * <p>Dirty marking (STALE detection) is handled externally by
 * {@code MaterializedViewConsistencyManager}, which reacts to base table segment changes
 * (add, replace, delete) and proactively marks affected partitions in
 * {@link MvRuntimeMetadata}.
 */
@TaskGenerator
public class MaterializedViewTaskGenerator extends BaseTaskGenerator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTaskGenerator.class);

  private static final String DEFAULT_BUCKET_PERIOD = "1d";

  @Override
  public String getTaskType() {
    return MaterializedViewTask.TASK_TYPE;
  }

  @Override
  public List<PinotTaskConfig> generateTasks(List<TableConfig> tableConfigs) {
    String taskType = MaterializedViewTask.TASK_TYPE;
    List<PinotTaskConfig> pinotTaskConfigs = new ArrayList<>();

    for (TableConfig tableConfig : tableConfigs) {
      String offlineTableName = tableConfig.getTableName();

      if (tableConfig.getTableType() != TableType.OFFLINE) {
        LOGGER.warn("Skip generating task: {} for non-OFFLINE table: {}", taskType, offlineTableName);
        continue;
      }
      LOGGER.info("Start generating task configs for table: {} for task: {}", offlineTableName, taskType);

      // Only schedule 1 task of this type per table
      Map<String, TaskState> incompleteTasks =
          TaskGeneratorUtils.getIncompleteTasks(taskType, offlineTableName, _clusterInfoAccessor);
      if (!incompleteTasks.isEmpty()) {
        LOGGER.warn("Found incomplete tasks: {} for table: {} and task type: {}. Skipping.",
            incompleteTasks.keySet(), offlineTableName, taskType);
        continue;
      }

      TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
      Preconditions.checkState(tableTaskConfig != null);
      Map<String, String> taskConfigs = tableTaskConfig.getConfigsForTaskType(taskType);
      Preconditions.checkState(taskConfigs != null, "Task config shouldn't be null for table: %s", offlineTableName);

      String definedSQL = taskConfigs.get(MaterializedViewTask.DEFINED_SQL_KEY);
      Preconditions.checkState(definedSQL != null && !definedSQL.isEmpty(),
          "definedSQL must be specified for table: %s", offlineTableName);

      String sourceTableName = MaterializedViewAnalyzer.extractSourceTableName(definedSQL);
      String sourceTableWithType = resolveSourceTableNameWithType(sourceTableName);

      // Bucket and buffer
      String bucketTimePeriod =
          taskConfigs.getOrDefault(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, DEFAULT_BUCKET_PERIOD);
      long bucketMs = TimeUtils.convertPeriodToMillis(bucketTimePeriod);
      String bufferTimePeriod =
          taskConfigs.getOrDefault(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY, "0d");
      long bufferMs = TimeUtils.convertPeriodToMillis(bufferTimePeriod);

      // Load upperExclusiveMs (watermark) and partitionInfos from MvRuntimeMetadata
      String mvTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(offlineTableName);
      HelixPropertyStore<ZNRecord> propertyStore =
          _clusterInfoAccessor.getPinotHelixResourceManager().getPropertyStore();
      long watermarkMs = getWatermarkMs(offlineTableName, sourceTableName, bucketMs, definedSQL);

      Stat rtStat = new Stat();
      MvRuntimeMetadata mvRuntime = MvRuntimeMetadataUtils.fetchWithVersion(
          propertyStore, mvTableWithType, rtStat);
      Map<Long, PartitionInfo> partitionInfos = new HashMap<>();
      int runtimeVersion = -1;
      if (mvRuntime != null) {
        partitionInfos = new HashMap<>(mvRuntime.getPartitions());
        runtimeVersion = rtStat.getVersion();
      }

      // ── Step 1a: Delete EXPIRED partitions (highest priority) ──
      PinotTaskConfig deleteTask = tryGenerateDeleteTask(offlineTableName, taskConfigs,
          partitionInfos, bucketMs);
      if (deleteTask != null) {
        pinotTaskConfigs.add(deleteTask);
        LOGGER.info("Generated DELETE task for table: {}", offlineTableName);
        continue;
      }

      // ── Step 1b: Overwrite STALE partitions (with precise fingerprint verification) ──
      PinotTaskConfig overwriteTask = tryGenerateOverwriteTask(offlineTableName, sourceTableName,
          sourceTableWithType, definedSQL, taskConfigs, partitionInfos, bucketMs,
          mvRuntime, runtimeVersion);
      if (overwriteTask != null) {
        pinotTaskConfigs.add(overwriteTask);
        LOGGER.info("Generated OVERWRITE task for table: {}", offlineTableName);
        continue;
      }

      // ── Step 2: Append new data (advance watermark) ──
      long windowStartMs = watermarkMs;
      long windowEndMs = windowStartMs + bucketMs;

      if (windowEndMs <= System.currentTimeMillis() - bufferMs) {
        PinotTaskConfig appendTask = buildTaskConfig(offlineTableName, sourceTableName,
            sourceTableWithType, definedSQL, taskConfigs, windowStartMs, windowEndMs,
            MaterializedViewTask.TASK_MODE_APPEND);
        pinotTaskConfigs.add(appendTask);
        LOGGER.info("Generated APPEND task for table: {} window [{}, {})", offlineTableName,
            windowStartMs, windowEndMs);
        continue;
      }

      LOGGER.debug("MV table {} is caught up (watermark={}), no dirty partitions.", offlineTableName, watermarkMs);
    }
    return pinotTaskConfigs;
  }

  /**
   * Step 1a: Finds the earliest EXPIRED partition and generates a DELETE task for it.
   * DELETE tasks only remove MV segments; no query is executed.
   *
   * @return a {@link PinotTaskConfig} for delete, or {@code null} if no EXPIRED partitions exist
   */
  private PinotTaskConfig tryGenerateDeleteTask(String mvTableName, Map<String, String> taskConfigs,
      Map<Long, PartitionInfo> partitionInfos, long bucketMs) {
    long earliestExpiredMs = Long.MAX_VALUE;
    for (Map.Entry<Long, PartitionInfo> entry : partitionInfos.entrySet()) {
      if (entry.getValue().getState() == PartitionState.EXPIRED && entry.getKey() < earliestExpiredMs) {
        earliestExpiredMs = entry.getKey();
      }
    }
    if (earliestExpiredMs == Long.MAX_VALUE) {
      return null;
    }
    long windowStartMs = earliestExpiredMs;
    long windowEndMs = windowStartMs + bucketMs;
    LOGGER.info("Found EXPIRED partition at {} for table: {}. Generating DELETE task for window [{}, {})",
        windowStartMs, mvTableName, windowStartMs, windowEndMs);

    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, mvTableName);
    configs.put(MaterializedViewTask.WINDOW_START_MS_KEY, String.valueOf(windowStartMs));
    configs.put(MaterializedViewTask.WINDOW_END_MS_KEY, String.valueOf(windowEndMs));
    configs.put(MaterializedViewTask.TASK_MODE_KEY, MaterializedViewTask.TASK_MODE_DELETE);
    configs.put(MinionConstants.UPLOAD_URL_KEY,
        _clusterInfoAccessor.getVipUrl() + "/segments");

    return new PinotTaskConfig(MaterializedViewTask.TASK_TYPE, configs);
  }

  /**
   * Step 1b: Finds the earliest STALE partition and generates an OVERWRITE task for it.
   *
   * <p>Before generating the task, re-computes the fingerprint for that partition to handle:
   * <ul>
   *   <li>STALE → EXPIRED promotion (all base segments deleted since marking)</li>
   *   <li>False positive recovery (fingerprint matches stored value → revert to VALID)</li>
   * </ul>
   *
   * @return a {@link PinotTaskConfig} for overwrite, or {@code null} if no actionable STALE
   *         partitions exist
   */
  private PinotTaskConfig tryGenerateOverwriteTask(String mvTableName, String sourceTableName,
      String sourceTableWithType, String definedSQL, Map<String, String> taskConfigs,
      Map<Long, PartitionInfo> partitionInfos, long bucketMs,
      MvRuntimeMetadata mvRuntime, int runtimeVersion) {
    long earliestStaleMs = Long.MAX_VALUE;
    for (Map.Entry<Long, PartitionInfo> entry : partitionInfos.entrySet()) {
      if (entry.getValue().getState() == PartitionState.STALE && entry.getKey() < earliestStaleMs) {
        earliestStaleMs = entry.getKey();
      }
    }
    if (earliestStaleMs == Long.MAX_VALUE) {
      return null;
    }

    long windowStartMs = earliestStaleMs;
    long windowEndMs = windowStartMs + bucketMs;
    PartitionInfo staleInfo = partitionInfos.get(earliestStaleMs);

    PartitionFingerprint currentFp = computeWindowFingerprint(sourceTableWithType, windowStartMs, windowEndMs);

    if (currentFp.getSegmentCount() == 0) {
      LOGGER.info("STALE partition [{}, {}) base data deleted for table: {}. Promoting to EXPIRED.",
          windowStartMs, windowEndMs, mvTableName);
      partitionInfos.put(earliestStaleMs, staleInfo.withState(PartitionState.EXPIRED));
      persistUpdatedPartitionInfos(mvTableName, partitionInfos, mvRuntime, runtimeVersion);
      return null;
    }

    if (currentFp.equals(staleInfo.getFingerprint())) {
      LOGGER.info("STALE partition [{}, {}) fingerprint matches for table: {}. "
          + "Reverting to VALID (false positive).", windowStartMs, windowEndMs, mvTableName);
      partitionInfos.put(earliestStaleMs, staleInfo.withState(PartitionState.VALID));
      persistUpdatedPartitionInfos(mvTableName, partitionInfos, mvRuntime, runtimeVersion);
      return null;
    }

    LOGGER.info("Confirmed STALE partition at {} for table: {}. Generating OVERWRITE task for window [{}, {})",
        windowStartMs, mvTableName, windowStartMs, windowEndMs);
    return buildTaskConfig(mvTableName, sourceTableName, sourceTableWithType, definedSQL,
        taskConfigs, windowStartMs, windowEndMs, MaterializedViewTask.TASK_MODE_OVERWRITE);
  }

  /**
   * Persists updated partitionInfos back to MV runtime in ZK (used for state promotions/reversions).
   */
  private void persistUpdatedPartitionInfos(String mvTableName, Map<Long, PartitionInfo> partitionInfos,
      MvRuntimeMetadata mvRuntime, int runtimeVersion) {
    MvFreshness freshness = MvRuntimeMetadata.computeFreshness(partitionInfos);
    MvRuntimeMetadata updated = new MvRuntimeMetadata(
        mvRuntime.getMvTableNameWithType(),
        mvRuntime.getWatermarkMs(),
        mvRuntime.getCoverageUpperMs(),
        freshness,
        partitionInfos);
    MvRuntimeMetadataUtils.persist(
        _clusterInfoAccessor.getPinotHelixResourceManager().getPropertyStore(),
        updated, runtimeVersion);
    LOGGER.info("Persisted partition state changes for MV table: {}", mvTableName);
  }

  /**
   * Builds a complete {@link PinotTaskConfig} for either APPEND or OVERWRITE mode.
   */
  private PinotTaskConfig buildTaskConfig(String mvTableName, String sourceTableName,
      String sourceTableWithType, String definedSQL, Map<String, String> taskConfigs,
      long windowStartMs, long windowEndMs, String taskMode) {
    String taskType = MaterializedViewTask.TASK_TYPE;

    PartitionFingerprint windowFingerprint =
        computeWindowFingerprint(sourceTableWithType, windowStartMs, windowEndMs);

    String sourceTimeColumn = resolveSourceTimeColumn(sourceTableName);
    DateTimeFormatSpec timeFormatSpec = resolveSourceTimeFormatSpec(sourceTableName, sourceTimeColumn);
    String windowStart = timeFormatSpec.fromMillisToFormat(windowStartMs);
    String windowEnd = timeFormatSpec.fromMillisToFormat(windowEndMs);
    String sqlWithTimeRange = appendTimeRange(definedSQL, sourceTimeColumn, windowStart, windowEnd);
    sqlWithTimeRange = ensureLimit(sqlWithTimeRange, MaterializedViewTask.DEFAULT_MV_QUERY_LIMIT);

    Map<String, String> configs = new HashMap<>();
    configs.put(MinionConstants.TABLE_NAME_KEY, mvTableName);
    configs.put(MaterializedViewTask.DEFINED_SQL_KEY, sqlWithTimeRange);
    configs.put(MaterializedViewTask.ORIGINAL_DEFINED_SQL_KEY, definedSQL);
    configs.put(MaterializedViewTask.WINDOW_START_MS_KEY, String.valueOf(windowStartMs));
    configs.put(MaterializedViewTask.WINDOW_END_MS_KEY, String.valueOf(windowEndMs));
    configs.put(MaterializedViewTask.SOURCE_TABLE_NAME_KEY, sourceTableName);
    configs.put(MaterializedViewTask.TASK_MODE_KEY, taskMode);
    configs.put(MinionConstants.UPLOAD_URL_KEY,
        _clusterInfoAccessor.getVipUrl() + "/segments");

    String maxNumRecords = taskConfigs.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
    if (maxNumRecords != null) {
      configs.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, maxNumRecords);
    }

    Map<Long, PartitionFingerprint> fingerprintMap = new HashMap<>();
    fingerprintMap.put(windowStartMs, windowFingerprint);
    configs.put(MaterializedViewTask.PARTITION_FINGERPRINTS_KEY,
        PartitionFingerprint.encodeMap(fingerprintMap));

    return new PinotTaskConfig(taskType, configs);
  }

  @Override
  public void validateTaskConfigs(TableConfig tableConfig, Schema schema, Map<String, String> taskConfigs) {
    MaterializedViewAnalyzer.analyze(
        taskConfigs.get(MaterializedViewTask.DEFINED_SQL_KEY),
        tableConfig, schema, taskConfigs, _clusterInfoAccessor);
  }

  /**
   * Resolves the time column for the source table by looking up its TableConfig.
   */
  private String resolveSourceTimeColumn(String rawSourceTableName) {
    // Try OFFLINE first, then REALTIME
    String sourceTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(rawSourceTableName);
    TableConfig sourceTableConfig = _clusterInfoAccessor.getTableConfig(sourceTableWithType);
    if (sourceTableConfig == null) {
      sourceTableWithType = TableNameBuilder.REALTIME.tableNameWithType(rawSourceTableName);
      sourceTableConfig = _clusterInfoAccessor.getTableConfig(sourceTableWithType);
    }
    Preconditions.checkState(sourceTableConfig != null,
        "Source table config not found for: %s", rawSourceTableName);

    String timeColumn = sourceTableConfig.getValidationConfig().getTimeColumnName();
    Preconditions.checkState(timeColumn != null && !timeColumn.isEmpty(),
        "Time column not configured for source table: %s", rawSourceTableName);
    return timeColumn;
  }

  /**
   * Resolves the {@link DateTimeFormatSpec} for the source table's time column by looking up
   * the table schema. This spec is used to convert millisecond-based watermarks to the
   * time column's native format (e.g. days since epoch for {@code 1:DAYS:EPOCH}).
   */
  private DateTimeFormatSpec resolveSourceTimeFormatSpec(String rawSourceTableName, String timeColumn) {
    String sourceTableWithType = resolveSourceTableNameWithType(rawSourceTableName);
    Schema sourceSchema = _clusterInfoAccessor.getTableSchema(sourceTableWithType);
    Preconditions.checkState(sourceSchema != null,
        "Schema not found for source table: %s", rawSourceTableName);

    DateTimeFieldSpec fieldSpec = sourceSchema.getSpecForTimeColumn(timeColumn);
    Preconditions.checkState(fieldSpec != null,
        "No DateTimeFieldSpec found for time column '%s' in source table: %s", timeColumn, rawSourceTableName);
    return fieldSpec.getFormatSpec();
  }

  /**
   * Resolves the raw format string (e.g. {@code "1:MILLISECONDS:EPOCH"}) for the source
   * table's time column, for persisting in {@link MvDefinitionMetadata.MvSplitSpec}.
   */
  private String resolveSourceTimeFormat(String rawSourceTableName, String timeColumn) {
    String sourceTableWithType = resolveSourceTableNameWithType(rawSourceTableName);
    Schema sourceSchema = _clusterInfoAccessor.getTableSchema(sourceTableWithType);
    Preconditions.checkState(sourceSchema != null,
        "Schema not found for source table: %s", rawSourceTableName);

    DateTimeFieldSpec fieldSpec = sourceSchema.getSpecForTimeColumn(timeColumn);
    Preconditions.checkState(fieldSpec != null,
        "No DateTimeFieldSpec found for time column '%s' in source table: %s", timeColumn, rawSourceTableName);
    return fieldSpec.getFormat();
  }

  /**
   * Appends a time-range WHERE clause to the SQL. The window values must already be in the
   * time column's native format (e.g. days since epoch, not milliseconds). If a WHERE clause
   * already exists, appends with AND; otherwise inserts before GROUP BY / ORDER BY / the
   * trailing semicolon.
   */
  static String appendTimeRange(String sql, String timeColumn, String windowStart, String windowEnd) {
    String timeFilter = timeColumn + " >= " + windowStart + " AND " + timeColumn + " < " + windowEnd;

    // Remove trailing semicolon for easier manipulation
    String trimmed = sql.trim();
    if (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }

    String upperSql = trimmed.toUpperCase();
    int whereIdx = upperSql.indexOf(" WHERE ");
    if (whereIdx >= 0) {
      // Find the end of the existing WHERE conditions (before GROUP BY, ORDER BY, LIMIT, or end)
      int insertPos = findClauseEnd(upperSql, whereIdx + 7);
      return trimmed.substring(0, insertPos) + " AND " + timeFilter + trimmed.substring(insertPos);
    }

    // No WHERE — insert before GROUP BY / ORDER BY / LIMIT / HAVING / end
    int insertPos = findClauseEnd(upperSql, upperSql.indexOf(" FROM ") + 6);
    // Move past the table name to find where to insert
    insertPos = findClauseEnd(upperSql, insertPos);
    return trimmed.substring(0, insertPos) + " WHERE " + timeFilter + trimmed.substring(insertPos);
  }

  /**
   * Ensures the SQL contains an explicit LIMIT clause. If the user's SQL already has one, it is
   * left unchanged; otherwise {@code defaultLimit} is appended. This prevents the broker from
   * applying its own default limit (typically 10) which would silently truncate MV results.
   */
  static String ensureLimit(String sql, int defaultLimit) {
    String trimmed = sql.trim();
    if (trimmed.endsWith(";")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1).trim();
    }
    if (trimmed.toUpperCase().contains(" LIMIT ")) {
      return trimmed;
    }
    return trimmed + " LIMIT " + defaultLimit;
  }

  /**
   * Finds the position of the next major SQL clause keyword (GROUP, ORDER, HAVING, LIMIT)
   * starting from {@code fromIdx}, or the end of the string if none found.
   */
  private static int findClauseEnd(String upperSql, int fromIdx) {
    String[] keywords = {" GROUP ", " ORDER ", " HAVING ", " LIMIT "};
    int minIdx = upperSql.length();
    for (String keyword : keywords) {
      int idx = upperSql.indexOf(keyword, fromIdx);
      if (idx >= 0 && idx < minIdx) {
        minIdx = idx;
      }
    }
    return minIdx;
  }

  /**
   * Reads the scheduling watermark from MvRuntimeMetadata or initialises it
   * on cold-start by finding the minimum segment start time from the source table
   * and aligning it to the bucket boundary.
   *
   * <p>On cold-start, {@code coverageUpperMs} is set to 0 so the broker will not
   * attempt split queries against the empty MV table. The first successful APPEND
   * (via the executor) will set both watermark and coverage to the window end.
   */
  private long getWatermarkMs(String mvTableName, String sourceTableName, long bucketMs, String definedSQL) {
    String mvTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(mvTableName);
    HelixPropertyStore<ZNRecord> propertyStore =
        _clusterInfoAccessor.getPinotHelixResourceManager().getPropertyStore();
    MvRuntimeMetadata runtime = MvRuntimeMetadataUtils.fetch(propertyStore, mvTableWithType);

    if (runtime != null && runtime.getWatermarkMs() > 0) {
      return runtime.getWatermarkMs();
    }

    // Cold-start: find the earliest segment start time from the source table
    String sourceTableWithType = resolveSourceTableNameWithType(sourceTableName);
    List<SegmentZKMetadata> segmentsZKMetadata = getSegmentsZKMetadataForTable(sourceTableWithType);

    long minStartTimeMs = Long.MAX_VALUE;
    for (SegmentZKMetadata segmentZKMetadata : segmentsZKMetadata) {
      long startTimeMs = segmentZKMetadata.getStartTimeMs();
      if (startTimeMs > 0) {
        minStartTimeMs = Math.min(minStartTimeMs, startTimeMs);
      }
    }
    Preconditions.checkState(minStartTimeMs != Long.MAX_VALUE,
        "No valid segments found in source table: %s for cold-start watermark", sourceTableName);

    long watermarkMs = (minStartTimeMs / bucketMs) * bucketMs;

    // coverageUpperMs = 0: no data has been appended yet, so the broker must
    // not issue split queries against this MV until the first APPEND completes.
    MvRuntimeMetadata newRuntime = new MvRuntimeMetadata(
        mvTableWithType, watermarkMs, 0L, MvFreshness.FRESH, new HashMap<>());
    MvRuntimeMetadataUtils.persist(propertyStore, newRuntime, -1);
    LOGGER.info("Cold-start: initialized MvRuntimeMetadata with watermark {} for MV table: {}",
        watermarkMs, mvTableWithType);

    // Initialize MvDefinitionMetadata with base table info and partition expression maps
    Schema mvSchema = _clusterInfoAccessor.getTableSchema(mvTableWithType);
    Map<String, String> partitionExprMaps = (mvSchema != null)
        ? MaterializedViewAnalyzer.extractPartitionExprMaps(definedSQL, mvSchema)
        : new HashMap<>();

    // Resolve split spec from the source table's time column
    String sourceTimeColumn = resolveSourceTimeColumn(sourceTableName);
    String sourceTimeFormat = resolveSourceTimeFormat(sourceTableName, sourceTimeColumn);
    MvDefinitionMetadata.MvSplitSpec splitSpec = new MvDefinitionMetadata.MvSplitSpec(
        sourceTimeColumn, sourceTimeFormat, bucketMs);

    MvDefinitionMetadata definition = new MvDefinitionMetadata(
        mvTableWithType,
        Collections.singletonList(sourceTableName),
        definedSQL,
        partitionExprMaps,
        splitSpec);
    MvDefinitionMetadataUtils.persist(propertyStore, definition, -1);
    LOGGER.info("Cold-start: initialized MvDefinitionMetadata for MV table: {} with source table: {}",
        mvTableWithType, sourceTableName);

    return watermarkMs;
  }

  /**
   * Resolves the source table name with type suffix. Tries OFFLINE first, then REALTIME.
   */
  private String resolveSourceTableNameWithType(String rawSourceTableName) {
    String sourceTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(rawSourceTableName);
    TableConfig sourceTableConfig = _clusterInfoAccessor.getTableConfig(sourceTableWithType);
    if (sourceTableConfig != null) {
      return sourceTableWithType;
    }
    sourceTableWithType = TableNameBuilder.REALTIME.tableNameWithType(rawSourceTableName);
    sourceTableConfig = _clusterInfoAccessor.getTableConfig(sourceTableWithType);
    Preconditions.checkState(sourceTableConfig != null,
        "Source table config not found for: %s", rawSourceTableName);
    return sourceTableWithType;
  }

  /**
   * Computes a {@link PartitionFingerprint} by fetching segments from ZK.
   */
  private PartitionFingerprint computeWindowFingerprint(String sourceTableWithType,
      long windowStartMs, long windowEndMs) {
    return computeWindowFingerprint(getSegmentsZKMetadataForTable(sourceTableWithType),
        windowStartMs, windowEndMs);
  }

  /**
   * Computes a {@link PartitionFingerprint} for the given time window from pre-fetched
   * segment metadata. Counts segments whose time range overlaps
   * {@code [windowStartMs, windowEndMs)} and sums their CRCs.
   */
  private PartitionFingerprint computeWindowFingerprint(List<SegmentZKMetadata> allSegments,
      long windowStartMs, long windowEndMs) {
    int segmentCount = 0;
    long crcChecksum = 0;
    for (SegmentZKMetadata seg : allSegments) {
      long segStartMs = seg.getStartTimeMs();
      long segEndMs = seg.getEndTimeMs();
      if (segStartMs < windowEndMs && segEndMs >= windowStartMs) {
        segmentCount++;
        crcChecksum += seg.getCrc();
      }
    }
    LOGGER.info("Computed partition fingerprint for window [{}, {}): segmentCount={}, crcChecksum={}",
        windowStartMs, windowEndMs, segmentCount, crcChecksum);
    return new PartitionFingerprint(segmentCount, crcChecksum);
  }
}
