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
import org.apache.helix.task.TaskState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.minion.MaterializedViewMetadata;
import org.apache.pinot.common.minion.MaterializedViewMetadataUtils;
import org.apache.pinot.common.minion.MaterializedViewTaskMetadata;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Task generator for {@link MaterializedViewTask}.
 *
 * <p>Unlike segment-conversion tasks, this generator does not scan source segments. It only
 * computes a time window and appends it to the user-defined SQL, producing a
 * {@link PinotTaskConfig} for the executor.
 *
 * <p>Three-step decision logic (evaluated per table, per schedule cycle):
 * <ol>
 *   <li><b>Overwrite STALE</b> – If any partition is marked {@link PartitionState#STALE},
 *       generate an {@code OVERWRITE} task for the earliest one. This has the highest
 *       priority to maintain consistency.</li>
 *   <li><b>Append</b> – If no STALE partitions exist and the watermark can advance (next
 *       window is outside the buffer period), generate a normal {@code APPEND} task.</li>
 *   <li><b>Scan for mismatches</b> – If the watermark cannot advance (MV is caught up),
 *       re-compute fingerprints for all {@link PartitionState#VALID} partitions and mark
 *       mismatches as {@link PartitionState#STALE} in ZK. No task is emitted; the stale
 *       partitions will be picked up in the next cycle (Step 1).</li>
 * </ol>
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

      // Load task metadata (ZNRecord + version for optimistic locking)
      ZNRecord znRecord = _clusterInfoAccessor.getMinionTaskMetadataZNRecord(
          MaterializedViewTask.TASK_TYPE, offlineTableName);
      long watermarkMs = getWatermarkMs(offlineTableName, sourceTableName, bucketMs, definedSQL);
      Map<Long, PartitionInfo> partitionInfos = new HashMap<>();
      int metadataVersion = -1;
      if (znRecord != null) {
        MaterializedViewTaskMetadata existingMeta = MaterializedViewTaskMetadata.fromZNRecord(znRecord);
        partitionInfos = existingMeta.getPartitionInfos();
        metadataVersion = znRecord.getVersion();
      }

      // ── Step 1: Overwrite STALE partitions (highest priority) ──
      PinotTaskConfig overwriteTask = tryGenerateOverwriteTask(offlineTableName, sourceTableName,
          sourceTableWithType, definedSQL, taskConfigs, partitionInfos, bucketMs);
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

      // ── Step 3: Watermark can't advance — scan VALID partitions for mismatches ──
      LOGGER.info("MV table {} is caught up (watermark={}). Scanning VALID partitions for data changes...",
          offlineTableName, watermarkMs);
      scanAndMarkStalePartitions(offlineTableName, sourceTableWithType, partitionInfos,
          bucketMs, metadataVersion, watermarkMs);
    }
    return pinotTaskConfigs;
  }

  /**
   * Step 1: Finds the earliest STALE partition and generates an OVERWRITE task for it.
   *
   * @return a {@link PinotTaskConfig} for overwrite, or {@code null} if no STALE partitions exist
   */
  private PinotTaskConfig tryGenerateOverwriteTask(String mvTableName, String sourceTableName,
      String sourceTableWithType, String definedSQL, Map<String, String> taskConfigs,
      Map<Long, PartitionInfo> partitionInfos, long bucketMs) {
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
    LOGGER.info("Found STALE partition at {} for table: {}. Generating OVERWRITE task for window [{}, {})",
        windowStartMs, mvTableName, windowStartMs, windowEndMs);
    return buildTaskConfig(mvTableName, sourceTableName, sourceTableWithType, definedSQL,
        taskConfigs, windowStartMs, windowEndMs, MaterializedViewTask.TASK_MODE_OVERWRITE);
  }

  /**
   * Step 3: Re-computes fingerprints for all VALID partitions and marks any with mismatched
   * fingerprints as STALE in ZK. No task is generated; the stale partitions will be handled
   * in the next scheduling cycle.
   */
  private void scanAndMarkStalePartitions(String mvTableName, String sourceTableWithType,
      Map<Long, PartitionInfo> partitionInfos, long bucketMs, int metadataVersion,
      long watermarkMs) {
    if (partitionInfos.isEmpty()) {
      return;
    }
    List<SegmentZKMetadata> allSegments = getSegmentsZKMetadataForTable(sourceTableWithType);
    boolean anyMarkedStale = false;

    for (Map.Entry<Long, PartitionInfo> entry : partitionInfos.entrySet()) {
      if (entry.getValue().getState() != PartitionState.VALID) {
        continue;
      }
      long partitionStartMs = entry.getKey();
      long partitionEndMs = partitionStartMs + bucketMs;
      PartitionFingerprint currentFp = computeWindowFingerprint(allSegments, partitionStartMs, partitionEndMs);
      PartitionFingerprint storedFp = entry.getValue().getFingerprint();

      if (!currentFp.equals(storedFp)) {
        LOGGER.info("Partition [{}, {}) fingerprint mismatch for table: {}. "
                + "Stored: {}, Current: {}. Marking STALE.",
            partitionStartMs, partitionEndMs, mvTableName, storedFp, currentFp);
        entry.setValue(entry.getValue().withState(PartitionState.STALE));
        anyMarkedStale = true;
      }
    }

    if (anyMarkedStale) {
      MaterializedViewTaskMetadata updatedMetadata =
          new MaterializedViewTaskMetadata(mvTableName, watermarkMs, partitionInfos);
      _clusterInfoAccessor.setMinionTaskMetadata(updatedMetadata,
          MaterializedViewTask.TASK_TYPE, metadataVersion);
      LOGGER.info("Updated task metadata with STALE partitions for table: {}", mvTableName);
    } else {
      LOGGER.info("All VALID partitions are consistent for table: {}", mvTableName);
    }
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
   * Reads the watermark from ZK or initialises it on cold-start by finding the minimum
   * segment start time from the source table and aligning it to the bucket boundary.
   */
  private long getWatermarkMs(String mvTableName, String sourceTableName, long bucketMs, String definedSQL) {
    ZNRecord znRecord = _clusterInfoAccessor.getMinionTaskMetadataZNRecord(
        MaterializedViewTask.TASK_TYPE, mvTableName);
    MaterializedViewTaskMetadata metadata =
        znRecord != null ? MaterializedViewTaskMetadata.fromZNRecord(znRecord) : null;

    if (metadata == null) {
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
      metadata = new MaterializedViewTaskMetadata(mvTableName, watermarkMs);
      _clusterInfoAccessor.setMinionTaskMetadata(metadata, MaterializedViewTask.TASK_TYPE, -1);
      LOGGER.info("Cold-start: initialized watermark to {} for MV table: {} from source table: {}",
          watermarkMs, mvTableName, sourceTableName);

      // Extract time column transformation mappings from the SQL
      String mvTableWithType = TableNameBuilder.OFFLINE.tableNameWithType(mvTableName);
      Schema mvSchema = _clusterInfoAccessor.getTableSchema(mvTableWithType);
      Map<String, String> partitionExprMaps = (mvSchema != null)
          ? MaterializedViewAnalyzer.extractPartitionExprMaps(definedSQL, mvSchema)
          : new HashMap<>();

      // Initialize MaterializedViewMetadata with base table info and partition expression maps
      MaterializedViewMetadata mvMetadata = new MaterializedViewMetadata(
          mvTableName,
          Collections.singletonList(sourceTableName),
          sourceTableName,
          definedSQL,
          new HashMap<>(), new HashMap<>(), partitionExprMaps);
      MaterializedViewMetadataUtils.persistMaterializedViewMetadata(
          _clusterInfoAccessor.getPinotHelixResourceManager().getPropertyStore(), mvMetadata, -1);
      LOGGER.info("Cold-start: initialized MaterializedViewMetadata for MV table: {} with source table: {}",
          mvTableName, sourceTableName);
    }
    return metadata.getWatermarkMs();
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
