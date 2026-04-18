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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadataCustomMapModifier;
import org.apache.pinot.common.minion.MvFreshness;
import org.apache.pinot.common.minion.MvRuntimeMetadata;
import org.apache.pinot.common.minion.MvRuntimeMetadataUtils;
import org.apache.pinot.common.minion.PartitionFingerprint;
import org.apache.pinot.common.minion.PartitionInfo;
import org.apache.pinot.common.minion.PartitionState;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.core.common.MinionConstants;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.core.minion.PinotTaskConfig;
import org.apache.pinot.minion.MinionConf;
import org.apache.pinot.minion.event.MinionEventObserver;
import org.apache.pinot.minion.event.MinionEventObservers;
import org.apache.pinot.minion.executor.MinionTaskZkMetadataManager;
import org.apache.pinot.plugin.minion.tasks.BaseTaskExecutor;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionResult;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionUtils;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.Obfuscator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Executor for {@link MaterializedViewTask}.
 *
 * <p>This task receives a SQL query with an appended time range (from the generator),
 * executes it via a pluggable {@link MvQueryExecutor} (e.g. gRPC, Arrow Flight),
 * and builds a segment from the query results for the MV table.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@code preProcess} – validates watermark against windowStartMs</li>
 *   <li>{@code executeTask} – queries broker, builds segment, uploads</li>
 *   <li>{@code postProcess} – advances watermark to windowEndMs</li>
 * </ol>
 */
public class MaterializedViewTaskExecutor extends BaseTaskExecutor {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedViewTaskExecutor.class);

  private final MinionTaskZkMetadataManager _minionTaskZkMetadataManager;
  private final MinionConf _minionConf;
  private final MvQueryExecutor _queryExecutor;

  /// CAS version for MvRuntimeMetadata. Set during preProcess.
  private int _runtimeExpectedVersion = Integer.MIN_VALUE;

  public MaterializedViewTaskExecutor(MinionTaskZkMetadataManager minionTaskZkMetadataManager,
      MinionConf minionConf, MvQueryExecutor queryExecutor) {
    _minionTaskZkMetadataManager = minionTaskZkMetadataManager;
    _minionConf = minionConf;
    _queryExecutor = queryExecutor;
  }

  public void preProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String taskMode = configs.getOrDefault(MaterializedViewTask.TASK_MODE_KEY,
        MaterializedViewTask.TASK_MODE_APPEND);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));

    // Fetch MvRuntimeMetadata for watermark validation and optimistic locking
    HelixPropertyStore<ZNRecord> propertyStore = MINION_CONTEXT.getHelixPropertyStore();
    org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
    MvRuntimeMetadata runtime = MvRuntimeMetadataUtils.fetchWithVersion(
        propertyStore, tableName, stat);

    if (runtime != null) {
      _runtimeExpectedVersion = stat.getVersion();

      if (MaterializedViewTask.TASK_MODE_APPEND.equals(taskMode)) {
        Preconditions.checkState(runtime.getWatermarkMs() <= windowStartMs,
            "watermarkMs %d should not be larger than windowStartMs %d for table %s",
            runtime.getWatermarkMs(), windowStartMs, tableName);
      } else if (MaterializedViewTask.TASK_MODE_OVERWRITE.equals(taskMode)) {
        PartitionInfo partitionInfo = runtime.getPartitions().get(windowStartMs);
        Preconditions.checkState(partitionInfo != null && partitionInfo.getState() == PartitionState.STALE,
            "Overwrite target partition %d should exist and be STALE for table %s",
            windowStartMs, tableName);
      } else if (MaterializedViewTask.TASK_MODE_DELETE.equals(taskMode)) {
        PartitionInfo partitionInfo = runtime.getPartitions().get(windowStartMs);
        Preconditions.checkState(partitionInfo != null && partitionInfo.getState() == PartitionState.EXPIRED,
            "Delete target partition %d should exist and be EXPIRED for table %s",
            windowStartMs, tableName);
      }
    } else {
      LOGGER.warn("MvRuntimeMetadata for table: {} not found; will be initialized in postProcess", tableName);
      _runtimeExpectedVersion = -1;
    }
  }

  @Override
  public SegmentConversionResult executeTask(PinotTaskConfig pinotTaskConfig)
      throws Exception {
    preProcess(pinotTaskConfig);

    MinionEventObserver eventObserver =
        MinionEventObservers.getInstance().getMinionEventObserver(pinotTaskConfig.getTaskId());

    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String taskType = pinotTaskConfig.getTaskType();
    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Starting task: {} with configs: {}", taskType, Obfuscator.DEFAULT.toJsonString(configs));
    }

    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));
    long windowEndMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_END_MS_KEY));
    String taskMode = configs.getOrDefault(MaterializedViewTask.TASK_MODE_KEY,
        MaterializedViewTask.TASK_MODE_APPEND);

    // DELETE mode: skip query execution, only remove existing MV segments
    if (MaterializedViewTask.TASK_MODE_DELETE.equals(taskMode)) {
      return executeDeleteTask(pinotTaskConfig, eventObserver, tableName, windowStartMs, windowEndMs);
    }

    String definedSQL = configs.get(MaterializedViewTask.DEFINED_SQL_KEY);
    LOGGER.info("MaterializedViewTask for table: {}, window: [{}, {}), SQL: {}",
        tableName, windowStartMs, windowEndMs, definedSQL);

    TableConfig tableConfig = getTableConfig(tableName);
    Schema schema = getSchema(tableName);

    eventObserver.notifyProgress(pinotTaskConfig, "Executing query for MV table: " + tableName);
    AuthProvider authProvider = resolveAuthProvider(configs);
    Map<String, String> authHeaders = AuthProviderUtils.makeAuthHeadersMap(authProvider);
    MvQueryExecutor.QueryResult queryResult = _queryExecutor.executeQuery(definedSQL, authHeaders);
    List<GenericRow> rows = convertToGenericRows(queryResult.getDataSchema(), queryResult.getRows());
    LOGGER.info("Query returned {} rows for table: {}", rows.size(), tableName);

    if (rows.isEmpty()) {
      LOGGER.info("No data returned for window [{}, {}) of table: {}. "
          + "Skipping segment creation and advancing watermark.", windowStartMs, windowEndMs, tableName);
      postProcess(pinotTaskConfig);
      return new SegmentConversionResult.Builder()
          .setTableNameWithType(tableName)
          .build();
    }

    String maxRecordsStr = configs.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
    int maxNumRecordsPerSegment = maxRecordsStr != null
        ? Integer.parseInt(maxRecordsStr)
        : MaterializedViewTask.DEFAULT_MAX_NUM_RECORDS_PER_SEGMENT;

    int totalRows = rows.size();
    int numSegments = (totalRows + maxNumRecordsPerSegment - 1) / maxNumRecordsPerSegment;
    LOGGER.info("Splitting {} rows into {} segment(s) (maxNumRecordsPerSegment={})",
        totalRows, numSegments, maxNumRecordsPerSegment);

    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);

    // Generate a per-attempt UUID so segment names are unique across retries of the same window.
    // Helix reuses the same subtask id (PinotTaskConfig#getTaskId) on every retry, so we cannot
    // rely on taskId for uniqueness — a retry after a partial upload would reproduce identical
    // names and the controller would reject the new lineage entry.
    String attemptId = UUID.randomUUID().toString();

    File tempDir = new File(FileUtils.getTempDirectory(),
        "mv_task_" + tableName + "_" + attemptId);
    FileUtils.forceMkdir(tempDir);

    try {
      // Phase 1: Build all segments and collect results
      List<SegmentConversionResult> conversionResults = new ArrayList<>();
      List<File> tarFiles = new ArrayList<>();

      for (int segIdx = 0; segIdx < numSegments; segIdx++) {
        int fromIndex = segIdx * maxNumRecordsPerSegment;
        int toIndex = Math.min(fromIndex + maxNumRecordsPerSegment, totalRows);
        List<GenericRow> chunk = rows.subList(fromIndex, toIndex);

        String segmentName = buildSegmentName(
            tableName, windowStartMs, windowEndMs, attemptId, segIdx);

        File segmentOutputDir = new File(tempDir, "segmentOutput_" + segIdx);
        FileUtils.forceMkdir(segmentOutputDir);

        SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
        segmentGeneratorConfig.setTableName(tableName);
        segmentGeneratorConfig.setOutDir(segmentOutputDir.getAbsolutePath());
        segmentGeneratorConfig.setSegmentName(segmentName);

        eventObserver.notifyProgress(pinotTaskConfig,
            String.format("Building segment %d/%d: %s (%d rows)", segIdx + 1, numSegments, segmentName, chunk.size()));

        SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
        driver.init(segmentGeneratorConfig, new GenericRowRecordReader(chunk));
        driver.build();

        File segmentDir = new File(segmentOutputDir, segmentName);
        Preconditions.checkState(segmentDir.exists(), "Segment generation failed for: %s", segmentName);

        File segmentTarFile = new File(tempDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
        TarCompressionUtils.createCompressedTarFile(segmentDir, segmentTarFile);

        conversionResults.add(new SegmentConversionResult.Builder()
            .setFile(segmentDir)
            .setSegmentName(segmentName)
            .setTableNameWithType(tableName)
            .build());
        tarFiles.add(segmentTarFile);
      }

      // Phase 2: Segment lineage — find old segments and start replace
      List<String> segmentsTo = new ArrayList<>();
      for (SegmentConversionResult r : conversionResults) {
        segmentsTo.add(r.getSegmentName());
      }

      String segmentPrefix = tableName + "_" + windowStartMs + "_" + windowEndMs;
      Set<String> allExistingSegments = SegmentConversionUtils.getSegmentNamesForTable(
          tableName, new URI(uploadURL).resolve("/"), authProvider);
      List<String> segmentsFrom = new ArrayList<>();
      for (String name : allExistingSegments) {
        if (name.equals(segmentPrefix) || name.startsWith(segmentPrefix + "_")) {
          segmentsFrom.add(name);
        }
      }

      String lineageEntryId = null;
      if (!segmentsFrom.isEmpty() || !segmentsTo.isEmpty()) {
        lineageEntryId = SegmentConversionUtils.startSegmentReplace(
            tableName, uploadURL,
            new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo),
            authProvider);
        LOGGER.info("Started segment replace for table: {}, lineageEntryId: {}, "
            + "segmentsFrom: {}, segmentsTo: {}", tableName, lineageEntryId, segmentsFrom, segmentsTo);
      }

      // Phase 3: Upload all segments
      for (int i = 0; i < conversionResults.size(); i++) {
        SegmentConversionResult result = conversionResults.get(i);
        File tarFile = tarFiles.get(i);
        String segmentName = result.getSegmentName();

        eventObserver.notifyProgress(pinotTaskConfig,
            String.format("Uploading segment %d/%d: %s", i + 1, numSegments, segmentName));

        List<Header> httpHeaders = getSegmentPushMetadataHeaders(pinotTaskConfig, authProvider, result);
        List<NameValuePair> parameters = getSegmentPushCommonParams(tableName);
        SegmentConversionUtils.uploadSegment(configs, httpHeaders, parameters, tableName, segmentName,
            uploadURL, tarFile);

        reportSegmentUploadMetrics(result.getFile(), tableName, taskType);

        LOGGER.info("Successfully uploaded segment {}/{}: {} for table: {}",
            i + 1, numSegments, segmentName, tableName);
      }

      // Phase 4: End segment replace to atomically swap lineage
      if (lineageEntryId != null) {
        SegmentConversionUtils.endSegmentReplace(
            tableName, uploadURL, lineageEntryId,
            _minionConf.getEndReplaceSegmentsTimeoutMs(), authProvider);
        LOGGER.info("Ended segment replace for table: {}, lineageEntryId: {}", tableName, lineageEntryId);
      }

      postProcess(pinotTaskConfig);

      return conversionResults.get(conversionResults.size() - 1);
    } finally {
      FileUtils.deleteQuietly(tempDir);
    }
  }

  /**
   * Handles DELETE mode: removes all existing MV segments for the given time window
   * via segment lineage replace (segmentsFrom=[old segments], segmentsTo=[]).
   * No query is executed and no new segments are created.
   */
  private SegmentConversionResult executeDeleteTask(PinotTaskConfig pinotTaskConfig,
      MinionEventObserver eventObserver, String tableName, long windowStartMs, long windowEndMs)
      throws Exception {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String uploadURL = configs.get(MinionConstants.UPLOAD_URL_KEY);
    AuthProvider authProvider = resolveAuthProvider(configs);

    LOGGER.info("DELETE task for table: {}, window: [{}, {}). Removing MV segments.",
        tableName, windowStartMs, windowEndMs);
    eventObserver.notifyProgress(pinotTaskConfig,
        "Deleting MV segments for window [" + windowStartMs + ", " + windowEndMs + ")");

    String segmentPrefix = tableName + "_" + windowStartMs + "_" + windowEndMs;
    Set<String> allExistingSegments = SegmentConversionUtils.getSegmentNamesForTable(
        tableName, new URI(uploadURL).resolve("/"), authProvider);
    List<String> segmentsFrom = new ArrayList<>();
    for (String name : allExistingSegments) {
      if (name.equals(segmentPrefix) || name.startsWith(segmentPrefix + "_")) {
        segmentsFrom.add(name);
      }
    }

    if (!segmentsFrom.isEmpty()) {
      List<String> segmentsTo = Collections.emptyList();
      String lineageEntryId = SegmentConversionUtils.startSegmentReplace(
          tableName, uploadURL,
          new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo),
          authProvider);
      LOGGER.info("Started segment delete-replace for table: {}, lineageEntryId: {}, segmentsFrom: {}",
          tableName, lineageEntryId, segmentsFrom);

      SegmentConversionUtils.endSegmentReplace(
          tableName, uploadURL, lineageEntryId,
          _minionConf.getEndReplaceSegmentsTimeoutMs(), authProvider);
      LOGGER.info("Ended segment delete-replace for table: {}, lineageEntryId: {}", tableName, lineageEntryId);
    } else {
      LOGGER.info("No existing segments found for prefix: {} in table: {}. Nothing to delete.",
          segmentPrefix, tableName);
    }

    postProcess(pinotTaskConfig);

    return new SegmentConversionResult.Builder()
        .setTableNameWithType(tableName)
        .build();
  }

  public void postProcess(PinotTaskConfig pinotTaskConfig) {
    Map<String, String> configs = pinotTaskConfig.getConfigs();
    String tableName = configs.get(MinionConstants.TABLE_NAME_KEY);
    String taskMode = configs.getOrDefault(MaterializedViewTask.TASK_MODE_KEY,
        MaterializedViewTask.TASK_MODE_APPEND);
    long windowStartMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_START_MS_KEY));
    long windowEndMs = Long.parseLong(configs.get(MaterializedViewTask.WINDOW_END_MS_KEY));

    updateMvRuntime(configs, tableName, taskMode, windowStartMs, windowEndMs);
  }

  /**
   * Updates {@link MvRuntimeMetadata} in a single CAS write, combining:
   * <ul>
   *   <li>partitions: set VALID with new fingerprint (APPEND/OVERWRITE) or remove (DELETE)</li>
   *   <li>watermarkMs: advance on APPEND only (generator scheduling)</li>
   *   <li>coverageUpperMs: advance on APPEND only (broker query coverage)</li>
   * </ul>
   */
  private void updateMvRuntime(Map<String, String> configs, String tableName,
      String taskMode, long windowStartMs, long windowEndMs) {
    HelixPropertyStore<ZNRecord> propertyStore = MINION_CONTEXT.getHelixPropertyStore();
    // Re-fetch with version at write time to avoid overwriting concurrent ConsistencyManager
    // updates (e.g. STALE markings) that arrived after preProcess captured _runtimeExpectedVersion.
    org.apache.zookeeper.data.Stat freshStat = new org.apache.zookeeper.data.Stat();
    MvRuntimeMetadata existing = MvRuntimeMetadataUtils.fetchWithVersion(propertyStore, tableName, freshStat);
    int writeVersion = (existing != null) ? freshStat.getVersion() : -1;

    Map<Long, PartitionInfo> mergedInfos;
    long existingWatermarkMs;
    long existingCoverageUpperMs;

    if (existing != null) {
      mergedInfos = new HashMap<>(existing.getPartitions());
      existingWatermarkMs = existing.getWatermarkMs();
      existingCoverageUpperMs = existing.getCoverageUpperMs();
    } else {
      mergedInfos = new HashMap<>();
      existingWatermarkMs = 0L;
      existingCoverageUpperMs = 0L;
    }

    long newWatermarkMs;
    long newCoverageUpperMs;

    if (MaterializedViewTask.TASK_MODE_DELETE.equals(taskMode)) {
      mergedInfos.remove(windowStartMs);
      newWatermarkMs = existingWatermarkMs;
      newCoverageUpperMs = existingCoverageUpperMs;
      LOGGER.info("DELETE mode: removed partition {} from MV runtime for table: {}", windowStartMs, tableName);
    } else {
      PartitionFingerprint newFingerprint = null;
      String fingerprintStr = configs.get(MaterializedViewTask.PARTITION_FINGERPRINTS_KEY);
      if (fingerprintStr != null && !fingerprintStr.isEmpty()) {
        Map<Long, PartitionFingerprint> taskFingerprints = PartitionFingerprint.decodeMap(fingerprintStr);
        newFingerprint = taskFingerprints.get(windowStartMs);
      }
      if (newFingerprint == null) {
        newFingerprint = new PartitionFingerprint(0, 0);
      }
      long nowMs = System.currentTimeMillis();
      PartitionInfo completedInfo = new PartitionInfo(PartitionState.VALID, newFingerprint, nowMs);
      mergedInfos.put(windowStartMs, completedInfo);
      LOGGER.info("Set partition {} to VALID (lastRefreshTime={}) for table: {}", windowStartMs, nowMs, tableName);

      if (MaterializedViewTask.TASK_MODE_APPEND.equals(taskMode)) {
        newWatermarkMs = windowEndMs;
        newCoverageUpperMs = windowEndMs;
        LOGGER.info("APPEND mode: advancing watermarkMs from {} to {}, coverageUpperMs from {} to {} for table: {}",
            existingWatermarkMs, newWatermarkMs, existingCoverageUpperMs, newCoverageUpperMs, tableName);
      } else {
        newWatermarkMs = existingWatermarkMs;
        newCoverageUpperMs = existingCoverageUpperMs;
        LOGGER.info("OVERWRITE mode: keeping watermarkMs at {}, coverageUpperMs at {} for table: {}",
            newWatermarkMs, newCoverageUpperMs, tableName);
      }
    }

    MvFreshness freshness = MvRuntimeMetadata.computeFreshness(mergedInfos);
    MvRuntimeMetadata updated = new MvRuntimeMetadata(
        tableName, newWatermarkMs, newCoverageUpperMs, freshness, mergedInfos);
    MvRuntimeMetadataUtils.persist(propertyStore, updated, writeVersion);

    LOGGER.info("Updated MV runtime for table: {} (partitions={}, watermarkMs={}, coverageUpperMs={})",
        tableName, mergedInfos.size(), newWatermarkMs, newCoverageUpperMs);
  }

  @Override
  protected SegmentZKMetadataCustomMapModifier getSegmentZKMetadataCustomMapModifier(
      PinotTaskConfig pinotTaskConfig, SegmentConversionResult segmentConversionResult) {
    return new SegmentZKMetadataCustomMapModifier(
        SegmentZKMetadataCustomMapModifier.ModifyMode.UPDATE, Collections.emptyMap());
  }

  /**
   * Converts raw query result rows into {@link GenericRow} objects using column names
   * from the {@link DataSchema}.
   */
  private List<GenericRow> convertToGenericRows(DataSchema dataSchema, List<Object[]> rows) {
    String[] columnNames = dataSchema.getColumnNames();
    List<GenericRow> genericRows = new ArrayList<>(rows.size());
    for (Object[] row : rows) {
      GenericRow genericRow = new GenericRow();
      for (int i = 0; i < columnNames.length; i++) {
        genericRow.putValue(columnNames[i], row[i]);
      }
      genericRows.add(genericRow);
    }
    return genericRows;
  }

  /**
   * Builds a segment name that is stable within a single attempt but unique across retries of the
   * same window. The {@code attemptId} must be a per-invocation value (e.g., a fresh UUID) and
   * must NOT be the Helix subtask id, which is reused across retries. Using the Helix subtask id
   * would reproduce identical names on retry, causing the controller to reject the new lineage
   * entry when segments from a previous partial attempt already exist.
   */
  @VisibleForTesting
  static String buildSegmentName(String tableName, long windowStartMs, long windowEndMs,
      String attemptId, int segIdx) {
    return tableName + "_" + windowStartMs + "_" + windowEndMs + "_" + attemptId + "_" + segIdx;
  }
}
