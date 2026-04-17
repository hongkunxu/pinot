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
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.io.FileUtils;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.minion.MvDefinitionMetadata;
import org.apache.pinot.common.minion.MvDefinitionMetadata.MvSplitSpec;
import org.apache.pinot.common.minion.MvDefinitionMetadataUtils;
import org.apache.pinot.common.minion.MvFreshness;
import org.apache.pinot.common.minion.MvRuntimeMetadata;
import org.apache.pinot.common.minion.MvRuntimeMetadataUtils;
import org.apache.pinot.common.utils.TarCompressionUtils;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * End-to-end integration tests for the Materialized View (MV) query rewrite pipeline.
 *
 * <p>These tests bypass the MV task generator/executor and directly set up the MV
 * infrastructure (MV table, segments, ZK metadata) to validate the broker's query
 * rewrite engine in a real cluster environment.
 *
 * <p>Phase 1 covers core rewrite scenarios:
 * <ol>
 *   <li>{@link #testFullRewriteWithExactMatch()} — MV covers full data range,
 *       {@code ExactSubsumptionStrategy} rewrites the query to the MV table.</li>
 *   <li>{@link #testSplitRewriteWithAggReagg()} — MV covers a historical range,
 *       {@code AggregationSubsumptionStrategy} merges MV + base table in split mode.</li>
 *   <li>{@link #testColdStartSkip()} — MV exists but {@code coverageUpperMs == 0},
 *       verifying the broker skips the MV during cold-start.</li>
 * </ol>
 *
 * <p>Phase 2 covers incremental updates and consistency:
 * <ol>
 *   <li>{@link #testIncrementalAppend()} — advance split MV coverage via new segments
 *       and ZK metadata update; verify the expanded coverage is respected.</li>
 *   <li>{@link #testFreshnessGating()} — mark MV as {@code STALE} → broker skips it;
 *       restore to {@code FRESH} → broker hits it again.</li>
 *   <li>{@link #testMvDefinitionDeletion()} — delete MV definition from ZK → broker
 *       no longer considers the MV as a candidate.</li>
 * </ol>
 *
 * <p>Phase 3 covers edge cases and advanced matching:
 * <ol>
 *   <li>{@link #testScanSubsumptionWithResidualFilter()} — SCAN-shaped MV with a
 *       user WHERE clause that becomes a residual filter on the MV table.</li>
 *   <li>{@link #testQueryWithLimitOffsetOrderBy()} — split mode query with
 *       ORDER BY + LIMIT + OFFSET; verify correct pagination and ordering.</li>
 *   <li>{@link #testMultipleMvCostSelection()} — two MVs match the same query at
 *       different costs; verify the lower-cost MV is selected.</li>
 * </ol>
 */
public class MaterializedViewClusterIntegrationTest extends BaseClusterIntegrationTest {

  private static final String SOURCE_TABLE_NAME = "mvSourceTable";
  private static final String MV_FULL_TABLE_NAME = "mvFullTable";
  private static final String MV_SPLIT_TABLE_NAME = "mvSplitTable";
  private static final String MV_COLD_TABLE_NAME = "mvColdTable";
  private static final String MV_SCAN_TABLE_NAME = "mvScanTable";
  private static final String MV_COST_TABLE_NAME = "mvCostTable";

  private static final String MV_FULL_TABLE_OFFLINE = MV_FULL_TABLE_NAME + "_OFFLINE";
  private static final String MV_SPLIT_TABLE_OFFLINE = MV_SPLIT_TABLE_NAME + "_OFFLINE";
  private static final String MV_COLD_TABLE_OFFLINE = MV_COLD_TABLE_NAME + "_OFFLINE";
  private static final String MV_SCAN_TABLE_OFFLINE = MV_SCAN_TABLE_NAME + "_OFFLINE";
  private static final String MV_COST_TABLE_OFFLINE = MV_COST_TABLE_NAME + "_OFFLINE";

  private static final String TIME_COLUMN = "DaysSinceEpoch";

  // Airline dataset spans DaysSinceEpoch ~16071–16101 (days since epoch)
  // 16071 * 86400000 = 1_388_534_400_000 (Jan 1, 2014)
  // 16102 * 86400000 = 1_391_212_800_000 (Feb 1, 2014)
  private static final long DATA_MIN_TIME_MS = 16071L * 86_400_000L;
  private static final long DATA_MAX_TIME_MS = 16102L * 86_400_000L;

  // Split boundary: MV covers first ~15 days, base table covers the rest
  private static final long SPLIT_BOUNDARY_MS = 16086L * 86_400_000L;

  private static final String[] CARRIERS = {"AA", "DL", "UA", "WN", "US", "B6", "OO", "EV", "MQ", "NK"};

  private PinotHelixResourceManager _helixResourceManager;
  private HelixPropertyStore<ZNRecord> _propertyStore;

  @Override
  protected String getTableName() {
    return SOURCE_TABLE_NAME;
  }

  @Override
  @Nullable
  protected String getSortedColumn() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getInvertedIndexColumns() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getNoDictionaryColumns() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getRangeIndexColumns() {
    return null;
  }

  @Override
  @Nullable
  protected List<String> getBloomFilterColumns() {
    return null;
  }

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(Broker.CONFIG_OF_BROKER_QUERY_ENABLE_MATERIALIZED_VIEW_REWRITE, true);
  }

  @BeforeClass
  public void setUp()
      throws Exception {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);

    startZk();
    startController();
    startBroker();
    startServer();

    _helixResourceManager = _controllerStarter.getHelixResourceManager();
    _propertyStore = _helixResourceManager.getPropertyStore();

    // --- Source table: load airline data ---
    Schema sourceSchema = createSchema();
    sourceSchema.setSchemaName(SOURCE_TABLE_NAME);
    addSchema(sourceSchema);

    TableConfig sourceTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(SOURCE_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();
    addTableConfig(sourceTableConfig);

    List<File> avroFiles = unpackAvroData(_tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, sourceTableConfig, sourceSchema,
        0, _segmentDir, _tarDir);
    uploadSegments(SOURCE_TABLE_NAME, _tarDir);
    waitForAllDocsLoaded(600_000L);

    // --- MV tables: build with synthetic pre-aggregated data ---
    setupFullRewriteMv();
    setupSplitRewriteMv();
    setupColdStartMv();
    setupScanMv();
    setupCostCompetitorMv();

    // Wait for MvMetadataCache ZK watchers to pick up changes
    Thread.sleep(5_000);
  }

  @AfterClass
  public void tearDown()
      throws Exception {
    dropOfflineTable(SOURCE_TABLE_NAME);
    dropOfflineTable(MV_FULL_TABLE_NAME);
    dropOfflineTable(MV_SPLIT_TABLE_NAME);
    dropOfflineTable(MV_COLD_TABLE_NAME);
    dropOfflineTable(MV_SCAN_TABLE_NAME);
    dropOfflineTable(MV_COST_TABLE_NAME);

    cleanupMvMetadata(MV_FULL_TABLE_OFFLINE);
    cleanupMvMetadata(MV_SPLIT_TABLE_OFFLINE);
    cleanupMvMetadata(MV_COLD_TABLE_OFFLINE);
    cleanupMvMetadata(MV_SCAN_TABLE_OFFLINE);
    cleanupMvMetadata(MV_COST_TABLE_OFFLINE);

    stopServer();
    stopBroker();
    stopController();
    stopZk();
    FileUtils.deleteDirectory(_tempDir);
  }

  // -----------------------------------------------------------------------
  //  Phase 1, Test 1: Full rewrite with ExactSubsumptionStrategy
  // -----------------------------------------------------------------------

  /**
   * Verifies that when an MV fully covers the source data and the query matches
   * the MV definition exactly, the broker rewrites the query to hit the MV table
   * via {@code FULL_REWRITE} execution mode.
   */
  @Test
  public void testFullRewriteWithExactMatch()
      throws Exception {
    String query = "SET useMaterializedView=true; "
        + "SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier";
    JsonNode response = postQuery(query);
    assertNoExceptions(response);

    String hitMv = getHitMv(response);
    assertNotNull(hitMv, "Expected MV to be hit for exact-match query. Response: " + response);
    assertEquals(hitMv, MV_FULL_TABLE_OFFLINE,
        "Expected hitMv to be the full-rewrite MV table");

    assertTrue(response.has("candidateMvs"), "Response should contain candidateMvs");
    JsonNode candidateMvs = response.get("candidateMvs");
    assertTrue(candidateMvs.isArray() && candidateMvs.size() > 0, "candidateMvs should be non-empty");

    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable, "resultTable should not be null");
    assertTrue(resultTable.get("rows").size() > 0, "Result should have rows");
  }

  // -----------------------------------------------------------------------
  //  Phase 1, Test 2: Split rewrite with AggregationSubsumptionStrategy
  // -----------------------------------------------------------------------

  /**
   * Verifies split-mode execution with re-aggregation. The MV covers only
   * historical data (up to {@code SPLIT_BOUNDARY_MS}). The broker merges
   * MV results with recent base table data.
   *
   * <p>Uses a query with {@code Origin} in GROUP BY, which matches the split
   * MV definition but not the full MV definition, ensuring this test targets
   * the split MV specifically.
   */
  @Test
  public void testSplitRewriteWithAggReagg()
      throws Exception {
    // This query matches the split MV (which groups by DaysSinceEpoch, Carrier, Origin)
    // via AggregationSubsumptionStrategy, but does NOT match the full MV (which only
    // groups by Carrier). The user query groups by (Carrier, Origin) which is a subset
    // of the split MV's (DaysSinceEpoch, Carrier, Origin), enabling re-aggregation.
    String query = "SET useMaterializedView=true; "
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier, Origin";
    JsonNode mvResponse = postQuery(query);
    assertNoExceptions(mvResponse);

    String hitMv = getHitMv(mvResponse);
    assertNotNull(hitMv, "Expected MV to be hit for split-mode query. Response: " + mvResponse);
    assertEquals(hitMv, MV_SPLIT_TABLE_OFFLINE,
        "Expected hitMv to be the split-rewrite MV table");

    JsonNode resultTable = mvResponse.get("resultTable");
    assertNotNull(resultTable, "resultTable should not be null");
    assertTrue(resultTable.get("rows").size() > 0, "Result should have rows");

    // Verify result count matches direct query
    String directQuery = "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier, Origin";
    JsonNode directResponse = postQuery(directQuery);
    assertNoExceptions(directResponse);

    int directRowCount = directResponse.get("resultTable").get("rows").size();
    int mvRowCount = resultTable.get("rows").size();
    assertEquals(mvRowCount, directRowCount,
        "Split MV query should return same number of groups as direct query");
  }

  // -----------------------------------------------------------------------
  //  Phase 1, Test 3: Cold-start skip
  // -----------------------------------------------------------------------

  /**
   * Verifies that the broker skips an MV with {@code coverageUpperMs == 0},
   * simulating a cold-start where the watermark is initialized but no APPEND
   * has completed.
   *
   * <p>The cold-start MV has the same definition as the split MV (grouping by
   * DaysSinceEpoch, Carrier, Origin). We use a query with {@code Carrier, Origin}
   * in GROUP BY, which would match both the split MV and the cold-start MV via
   * {@code AggregationSubsumptionStrategy}. The cold-start MV should be skipped
   * because its {@code coverageUpperMs == 0}, and the split MV should be hit instead.
   */
  @Test
  public void testColdStartSkip()
      throws Exception {
    // This query matches both the split MV and the cold-start MV structurally.
    // Only the split MV should be hit; the cold-start MV should be skipped.
    String query = "SET useMaterializedView=true; "
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier, Origin";
    JsonNode response = postQuery(query);
    assertNoExceptions(response);

    String hitMv = getHitMv(response);
    assertNotNull(hitMv, "Expected an MV to be hit. Response: " + response);
    assertNotEquals(hitMv, MV_COLD_TABLE_OFFLINE,
        "Cold-start MV should NOT be hit (coverageUpperMs == 0)");
    assertEquals(hitMv, MV_SPLIT_TABLE_OFFLINE,
        "Expected the split MV to be hit instead of the cold-start MV");

    // Verify candidateMvs contains the cold-start MV (it was evaluated but skipped)
    assertTrue(containsCandidate(response, MV_COLD_TABLE_OFFLINE),
        "Cold-start MV should appear in candidateMvs (it was structurally evaluated)");

    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable, "resultTable should not be null");
    assertTrue(resultTable.get("rows").size() > 0,
        "Query should still return results via the split MV");
  }

  // -----------------------------------------------------------------------
  //  Phase 2, Test 4: Incremental append — coverage boundary advances
  // -----------------------------------------------------------------------

  /**
   * Simulates an incremental APPEND by uploading additional MV segments that cover
   * days 16086–16095 (previously only 16071–16085 were materialized), then advancing
   * {@code coverageUpperMs} in ZK. Verifies that the broker respects the expanded
   * coverage boundary and still produces correct split-mode results.
   *
   * <p>This test depends on the split MV state established by {@link #setUp()}.
   */
  @Test(dependsOnMethods = "testSplitRewriteWithAggReagg")
  public void testIncrementalAppend()
      throws Exception {
    long extendedBoundaryMs = 16096L * 86_400_000L;

    // Upload additional MV segments for the newly materialized time window
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName(MV_SPLIT_TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Origin", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MV_SPLIT_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();

    String[] origins = {"SFO", "LAX", "JFK", "ORD", "ATL"};
    List<GenericRow> rows = new ArrayList<>();
    for (int day = 16086; day < 16096; day++) {
      for (String carrier : CARRIERS) {
        for (String origin : origins) {
          GenericRow row = new GenericRow();
          row.putValue(TIME_COLUMN, day);
          row.putValue("Carrier", carrier);
          row.putValue("Origin", origin);
          row.putValue("sum_ArrDelayMinutes",
              100.0 + (day + carrier.hashCode() + origin.hashCode()) % 200);
          rows.add(row);
        }
      }
    }
    buildAndUploadSegment(mvTableConfig, mvSchema, rows, MV_SPLIT_TABLE_NAME, "mvSplitSeg2");

    long previousCount = getCurrentCountStarResult(MV_SPLIT_TABLE_NAME);
    TestUtils.waitForCondition(
        () -> getCurrentCountStarResult(MV_SPLIT_TABLE_NAME) > previousCount,
        100L, 60_000L, "New MV split segments not loaded", Duration.ofSeconds(6));

    // Advance coverageUpperMs in ZK to reflect the newly materialized window
    MvRuntimeMetadata updatedRuntime = new MvRuntimeMetadata(
        MV_SPLIT_TABLE_OFFLINE, extendedBoundaryMs, extendedBoundaryMs,
        MvFreshness.FRESH, new HashMap<>());
    MvRuntimeMetadataUtils.persist(_propertyStore, updatedRuntime, -1);

    // Wait for ZK watcher to propagate the coverage update to the broker cache
    Thread.sleep(5_000);

    // Query and verify: the split MV should still be hit with the advanced boundary
    String query = "SET useMaterializedView=true; "
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier, Origin";
    JsonNode response = postQuery(query);
    assertNoExceptions(response);

    String hitMv = getHitMv(response);
    assertNotNull(hitMv, "MV should be hit after incremental append. Response: " + response);
    assertEquals(hitMv, MV_SPLIT_TABLE_OFFLINE);

    JsonNode resultTable = response.get("resultTable");
    assertNotNull(resultTable);
    assertTrue(resultTable.get("rows").size() > 0);

    // Compare with direct query to verify correctness
    String directQuery = "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier, Origin";
    JsonNode directResponse = postQuery(directQuery);
    assertNoExceptions(directResponse);
    assertEquals(
        resultTable.get("rows").size(),
        directResponse.get("resultTable").get("rows").size(),
        "Row count should match direct query after incremental append");
  }

  // -----------------------------------------------------------------------
  //  Phase 2, Test 5: Freshness gating — STALE MV is skipped
  // -----------------------------------------------------------------------

  /**
   * Verifies that the broker skips an MV when its freshness is set to {@code STALE},
   * and resumes using it once freshness is restored to {@code FRESH}.
   *
   * <p>This exercises the {@code isEligible()} freshness gate in
   * {@code MvQueryRewriteEngine}. We use the full-rewrite MV since it has a
   * unique query pattern (GROUP BY Carrier only), making it easy to verify
   * whether the MV is hit or not.
   */
  @Test(dependsOnMethods = "testFullRewriteWithExactMatch")
  public void testFreshnessGating()
      throws Exception {
    String query = "SET useMaterializedView=true; "
        + "SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier";

    // Step 1: Verify MV is currently hit (precondition)
    JsonNode preResponse = postQuery(query);
    assertNoExceptions(preResponse);
    assertEquals(getHitMv(preResponse), MV_FULL_TABLE_OFFLINE,
        "Precondition: full MV should be hit before staleness update");

    // Step 2: Mark the full MV as STALE
    MvRuntimeMetadata staleRuntime = new MvRuntimeMetadata(
        MV_FULL_TABLE_OFFLINE, DATA_MAX_TIME_MS, DATA_MAX_TIME_MS,
        MvFreshness.STALE, new HashMap<>());
    MvRuntimeMetadataUtils.persist(_propertyStore, staleRuntime, -1);

    // Wait for ZK watcher propagation
    Thread.sleep(5_000);

    // Step 3: Query again — the MV should NOT be hit (freshness gate rejects it)
    JsonNode staleResponse = postQuery(query);
    assertNoExceptions(staleResponse);
    String staleHitMv = getHitMv(staleResponse);
    if (staleHitMv != null) {
      assertNotEquals(staleHitMv, MV_FULL_TABLE_OFFLINE,
          "STALE MV should NOT be hit");
    }

    // Query should still return results (from base table or another MV)
    JsonNode staleResultTable = staleResponse.get("resultTable");
    assertNotNull(staleResultTable);
    assertTrue(staleResultTable.get("rows").size() > 0,
        "Query should still return results when MV is STALE");

    // Step 4: Restore freshness to FRESH
    MvRuntimeMetadata freshRuntime = new MvRuntimeMetadata(
        MV_FULL_TABLE_OFFLINE, DATA_MAX_TIME_MS, DATA_MAX_TIME_MS,
        MvFreshness.FRESH, new HashMap<>());
    MvRuntimeMetadataUtils.persist(_propertyStore, freshRuntime, -1);

    // Wait for ZK watcher propagation
    Thread.sleep(5_000);

    // Step 5: Query again — the MV should be hit again
    JsonNode freshResponse = postQuery(query);
    assertNoExceptions(freshResponse);
    assertEquals(getHitMv(freshResponse), MV_FULL_TABLE_OFFLINE,
        "MV should be hit again after freshness restored to FRESH");
  }

  // -----------------------------------------------------------------------
  //  Phase 2, Test 6: MV definition deletion — broker stops rewriting
  // -----------------------------------------------------------------------

  /**
   * Verifies that when an MV's definition is deleted from ZooKeeper, the broker
   * removes it from the cache and no longer considers it as a rewrite candidate.
   *
   * <p>Uses the cold-start MV (which has no segments and is not hit anyway) to
   * avoid interfering with other tests. After deletion, the MV should disappear
   * from {@code candidateMvs} entirely.
   */
  @Test(dependsOnMethods = "testColdStartSkip")
  public void testMvDefinitionDeletion()
      throws Exception {
    String query = "SET useMaterializedView=true; "
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier, Origin";

    // Step 1: Verify the cold MV is in candidateMvs (precondition)
    JsonNode preResponse = postQuery(query);
    assertNoExceptions(preResponse);
    assertTrue(containsCandidate(preResponse, MV_COLD_TABLE_OFFLINE),
        "Precondition: cold MV should appear in candidateMvs");

    // Step 2: Delete the cold MV definition from ZK
    MvDefinitionMetadataUtils.delete(_propertyStore, MV_COLD_TABLE_OFFLINE);
    MvRuntimeMetadataUtils.delete(_propertyStore, MV_COLD_TABLE_OFFLINE);

    // Wait for ZK watcher propagation
    Thread.sleep(5_000);

    // Step 3: Query again — the cold MV should no longer appear in candidateMvs
    JsonNode postResponse = postQuery(query);
    assertNoExceptions(postResponse);
    assertFalse(containsCandidate(postResponse, MV_COLD_TABLE_OFFLINE),
        "Deleted MV should NOT appear in candidateMvs after definition removal");

    // The split MV should still work
    String hitMv = getHitMv(postResponse);
    assertNotNull(hitMv, "Split MV should still be available. Response: " + postResponse);
    assertEquals(hitMv, MV_SPLIT_TABLE_OFFLINE);
  }

  // -----------------------------------------------------------------------
  //  Phase 3, Test 7: Scan subsumption with residual WHERE filter
  // -----------------------------------------------------------------------

  /**
   * Verifies that a SCAN-shaped MV (no GROUP BY, no aggregation) is matched by
   * {@code ScanSubsumptionStrategy} when the user query selects a subset of the
   * MV's columns and adds a WHERE filter that references only MV columns.
   *
   * <p>The scan MV stores {@code Carrier, Origin, Dest, ArrDelayMinutes} from the
   * source table. The user query selects {@code Carrier, ArrDelayMinutes} with
   * {@code WHERE Origin = 'SFO'} — the WHERE clause becomes a residual filter
   * on the MV table.
   */
  @Test
  public void testScanSubsumptionWithResidualFilter()
      throws Exception {
    // Query with WHERE filter — should hit the scan MV via ScanSubsumptionStrategy
    String query = "SET useMaterializedView=true; "
        + "SELECT Carrier, ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "WHERE Origin = 'SFO'";
    JsonNode mvResponse = postQuery(query);
    assertNoExceptions(mvResponse);

    String hitMv = getHitMv(mvResponse);
    assertNotNull(hitMv, "Expected scan MV to be hit. Response: " + mvResponse);
    assertEquals(hitMv, MV_SCAN_TABLE_OFFLINE,
        "Expected hitMv to be the scan MV table");

    JsonNode resultTable = mvResponse.get("resultTable");
    assertNotNull(resultTable, "resultTable should not be null");
    assertTrue(resultTable.get("rows").size() > 0, "Result should have rows");

    // Verify result count matches direct query (without MV rewrite)
    String directQuery = "SELECT Carrier, ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " WHERE Origin = 'SFO'";
    JsonNode directResponse = postQuery(directQuery);
    assertNoExceptions(directResponse);

    assertEquals(
        resultTable.get("rows").size(),
        directResponse.get("resultTable").get("rows").size(),
        "Scan MV query should return same row count as direct query");
  }

  // -----------------------------------------------------------------------
  //  Phase 3, Test 8: Split mode with ORDER BY + LIMIT + OFFSET
  // -----------------------------------------------------------------------

  /**
   * Verifies that a split-mode query with ORDER BY, LIMIT, and OFFSET produces
   * correct results. The broker must collect enough rows from both MV and base
   * table sides to satisfy the OFFSET + LIMIT requirement, then apply final
   * ordering and pagination during the merge phase.
   */
  @Test
  public void testQueryWithLimitOffsetOrderBy()
      throws Exception {
    String mvQuery = "SET useMaterializedView=true; "
        + "SELECT Carrier, Origin, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY Carrier, Origin "
        + "ORDER BY sum_ArrDelayMinutes DESC "
        + "LIMIT 5 OFFSET 3";
    JsonNode mvResponse = postQuery(mvQuery);
    assertNoExceptions(mvResponse);

    String hitMv = getHitMv(mvResponse);
    assertNotNull(hitMv, "Expected MV to be hit for LIMIT/OFFSET query. Response: " + mvResponse);
    assertEquals(hitMv, MV_SPLIT_TABLE_OFFLINE);

    JsonNode mvRows = mvResponse.get("resultTable").get("rows");
    assertNotNull(mvRows);
    assertEquals(mvRows.size(), 5, "LIMIT 5 should return exactly 5 rows");

    for (int i = 1; i < mvRows.size(); i++) {
      double prev = mvRows.get(i - 1).get(2).asDouble();
      double curr = mvRows.get(i).get(2).asDouble();
      assertTrue(prev >= curr,
          "Results should be ordered DESC by sum_ArrDelayMinutes: row " + (i - 1)
              + " (" + prev + ") < row " + i + " (" + curr + ")");
    }
  }

  // -----------------------------------------------------------------------
  //  Phase 3, Test 9: Multiple MVs — cost-based selection
  // -----------------------------------------------------------------------

  /**
   * Verifies that when two MVs match the same user query, the one with the lower
   * rewrite cost is selected.
   *
   * <p>The full MV matches {@code GROUP BY Carrier} via {@code ExactSubsumptionStrategy}
   * (cost 0.0). The cost-competitor MV (GROUP BY {@code DaysSinceEpoch, Carrier},
   * full coverage, no split spec) matches the same query via
   * {@code AggregationSubsumptionStrategy} (cost 6.0). The full MV should win.
   */
  @Test
  public void testMultipleMvCostSelection()
      throws Exception {
    String query = "SET useMaterializedView=true; "
        + "SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier";
    JsonNode response = postQuery(query);
    assertNoExceptions(response);

    // Both MVs should appear as candidates
    assertTrue(containsCandidate(response, MV_FULL_TABLE_OFFLINE),
        "Full MV should be a candidate");
    assertTrue(containsCandidate(response, MV_COST_TABLE_OFFLINE),
        "Cost-competitor MV should be a candidate");

    // The full MV (EXACT, cost 0.0) should win over the cost competitor (AGG_REAGG, cost 6.0)
    String hitMv = getHitMv(response);
    assertNotNull(hitMv, "Expected an MV to be hit. Response: " + response);
    assertEquals(hitMv, MV_FULL_TABLE_OFFLINE,
        "Full MV (EXACT, cost 0.0) should be selected over cost competitor (AGG_REAGG, cost 6.0)");
  }

  // -----------------------------------------------------------------------
  //  MV table setup
  // -----------------------------------------------------------------------

  /**
   * Full-rewrite MV: no split spec, covers all data.
   * Definition: {@code SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes
   * FROM mvSourceTable GROUP BY Carrier}
   */
  private void setupFullRewriteMv()
      throws Exception {
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName(MV_FULL_TABLE_NAME)
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(mvSchema);

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MV_FULL_TABLE_NAME)
        .setNumReplicas(1)
        .build();
    addTableConfig(mvTableConfig);

    // Build synthetic pre-aggregated rows: one row per carrier
    List<GenericRow> rows = new ArrayList<>();
    for (String carrier : CARRIERS) {
      GenericRow row = new GenericRow();
      row.putValue("Carrier", carrier);
      row.putValue("sum_ArrDelayMinutes", 1000.0 + carrier.hashCode() % 500);
      rows.add(row);
    }
    buildAndUploadSegment(mvTableConfig, mvSchema, rows, MV_FULL_TABLE_NAME, "mvFullSeg");

    waitForAnyDocLoaded(MV_FULL_TABLE_NAME, 60_000L);

    String definedSql = "SELECT Carrier, SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " GROUP BY Carrier";
    MvDefinitionMetadata definition = new MvDefinitionMetadata(
        MV_FULL_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        null);
    MvDefinitionMetadataUtils.persist(_propertyStore, definition, -1);

    MvRuntimeMetadata runtime = new MvRuntimeMetadata(
        MV_FULL_TABLE_OFFLINE, DATA_MAX_TIME_MS, DATA_MAX_TIME_MS,
        MvFreshness.FRESH, new HashMap<>());
    MvRuntimeMetadataUtils.persist(_propertyStore, runtime, -1);
  }

  /**
   * Split-rewrite MV: has split spec, covers historical data up to SPLIT_BOUNDARY_MS.
   * Definition: {@code SELECT DaysSinceEpoch, Carrier, Origin,
   * SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes
   * FROM mvSourceTable GROUP BY DaysSinceEpoch, Carrier, Origin}
   *
   * <p>Includes {@code Origin} to differentiate from the full MV, ensuring test 2's
   * query (with {@code GROUP BY Carrier, Origin}) hits this MV via
   * {@code AggregationSubsumptionStrategy} and not the full MV.
   */
  private void setupSplitRewriteMv()
      throws Exception {
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName(MV_SPLIT_TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Origin", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(mvSchema);

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MV_SPLIT_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();
    addTableConfig(mvTableConfig);

    // Build synthetic rows: one per (day, carrier, origin) combination
    String[] origins = {"SFO", "LAX", "JFK", "ORD", "ATL"};
    List<GenericRow> rows = new ArrayList<>();
    for (int day = 16071; day < 16086; day++) {
      for (String carrier : CARRIERS) {
        for (String origin : origins) {
          GenericRow row = new GenericRow();
          row.putValue(TIME_COLUMN, day);
          row.putValue("Carrier", carrier);
          row.putValue("Origin", origin);
          row.putValue("sum_ArrDelayMinutes",
              100.0 + (day + carrier.hashCode() + origin.hashCode()) % 200);
          rows.add(row);
        }
      }
    }
    buildAndUploadSegment(mvTableConfig, mvSchema, rows, MV_SPLIT_TABLE_NAME, "mvSplitSeg");

    waitForAnyDocLoaded(MV_SPLIT_TABLE_NAME, 60_000L);

    String definedSql = "SELECT " + TIME_COLUMN + ", Carrier, Origin, "
        + "SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY " + TIME_COLUMN + ", Carrier, Origin";
    MvSplitSpec splitSpec = new MvSplitSpec(TIME_COLUMN, "1:DAYS:EPOCH", 86_400_000L);
    MvDefinitionMetadata definition = new MvDefinitionMetadata(
        MV_SPLIT_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        splitSpec);
    MvDefinitionMetadataUtils.persist(_propertyStore, definition, -1);

    MvRuntimeMetadata runtime = new MvRuntimeMetadata(
        MV_SPLIT_TABLE_OFFLINE, SPLIT_BOUNDARY_MS, SPLIT_BOUNDARY_MS,
        MvFreshness.FRESH, new HashMap<>());
    MvRuntimeMetadataUtils.persist(_propertyStore, runtime, -1);
  }

  /**
   * Cold-start MV: definition with split spec exists, watermark initialized,
   * but coverageUpperMs == 0.  The cold-start check in {@code resolvePlan} only
   * triggers for incremental MVs (with a split spec), so a split spec is required
   * here to exercise the cold-start skip path.
   */
  private void setupColdStartMv()
      throws Exception {
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName(MV_COLD_TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Origin", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(mvSchema);

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MV_COLD_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();
    addTableConfig(mvTableConfig);
    // No segments uploaded — empty MV table (pre-APPEND state)

    // Same definition as the split MV but with coverageUpperMs == 0 to test cold-start
    String definedSql = "SELECT " + TIME_COLUMN + ", Carrier, Origin, "
        + "SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY " + TIME_COLUMN + ", Carrier, Origin";
    MvSplitSpec splitSpec = new MvSplitSpec(TIME_COLUMN, "1:DAYS:EPOCH", 86_400_000L);
    MvDefinitionMetadata definition = new MvDefinitionMetadata(
        MV_COLD_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        splitSpec);
    MvDefinitionMetadataUtils.persist(_propertyStore, definition, -1);

    // watermarkMs > 0 but coverageUpperMs == 0 → cold-start
    MvRuntimeMetadata runtime = new MvRuntimeMetadata(
        MV_COLD_TABLE_OFFLINE, DATA_MIN_TIME_MS, 0L,
        MvFreshness.FRESH, new HashMap<>());
    MvRuntimeMetadataUtils.persist(_propertyStore, runtime, -1);
  }

  /**
   * Scan MV: no GROUP BY, no aggregation. Stores a projection of the source table
   * columns for scan-subsumption matching.
   * Definition: {@code SELECT Carrier, Origin, Dest, ArrDelayMinutes FROM mvSourceTable}
   */
  private void setupScanMv()
      throws Exception {
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName(MV_SCAN_TABLE_NAME)
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Origin", FieldSpec.DataType.STRING)
        .addSingleValueDimension("Dest", FieldSpec.DataType.STRING)
        .addMetric("ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(mvSchema);

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MV_SCAN_TABLE_NAME)
        .setNumReplicas(1)
        .build();
    addTableConfig(mvTableConfig);

    // Build synthetic scan rows with representative data
    String[] origins = {"SFO", "LAX", "JFK", "ORD", "ATL"};
    String[] dests = {"LAX", "SFO", "ORD", "JFK", "DFW"};
    List<GenericRow> rows = new ArrayList<>();
    for (String carrier : CARRIERS) {
      for (int i = 0; i < origins.length; i++) {
        GenericRow row = new GenericRow();
        row.putValue("Carrier", carrier);
        row.putValue("Origin", origins[i]);
        row.putValue("Dest", dests[i]);
        row.putValue("ArrDelayMinutes", 10.0 + (carrier.hashCode() + i) % 50);
        rows.add(row);
      }
    }
    buildAndUploadSegment(mvTableConfig, mvSchema, rows, MV_SCAN_TABLE_NAME, "mvScanSeg");

    waitForAnyDocLoaded(MV_SCAN_TABLE_NAME, 60_000L);

    String definedSql = "SELECT Carrier, Origin, Dest, ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME;
    MvDefinitionMetadata definition = new MvDefinitionMetadata(
        MV_SCAN_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        null);
    MvDefinitionMetadataUtils.persist(_propertyStore, definition, -1);

    MvRuntimeMetadata runtime = new MvRuntimeMetadata(
        MV_SCAN_TABLE_OFFLINE, DATA_MAX_TIME_MS, DATA_MAX_TIME_MS,
        MvFreshness.FRESH, new HashMap<>());
    MvRuntimeMetadataUtils.persist(_propertyStore, runtime, -1);
  }

  /**
   * Cost-competitor MV: groups by {@code DaysSinceEpoch, Carrier} with full coverage
   * and no split spec. This MV matches {@code GROUP BY Carrier} queries via
   * {@code AggregationSubsumptionStrategy} (cost 6.0), competing with the full MV
   * which matches via {@code ExactSubsumptionStrategy} (cost 0.0).
   */
  private void setupCostCompetitorMv()
      throws Exception {
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName(MV_COST_TABLE_NAME)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.INT, "1:DAYS:EPOCH", "1:DAYS")
        .addSingleValueDimension("Carrier", FieldSpec.DataType.STRING)
        .addMetric("sum_ArrDelayMinutes", FieldSpec.DataType.DOUBLE)
        .build();
    addSchema(mvSchema);

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(MV_COST_TABLE_NAME)
        .setTimeColumnName(TIME_COLUMN)
        .setNumReplicas(1)
        .build();
    addTableConfig(mvTableConfig);

    // Build synthetic rows: one per (day, carrier) combination
    List<GenericRow> rows = new ArrayList<>();
    for (int day = 16071; day <= 16101; day++) {
      for (String carrier : CARRIERS) {
        GenericRow row = new GenericRow();
        row.putValue(TIME_COLUMN, day);
        row.putValue("Carrier", carrier);
        row.putValue("sum_ArrDelayMinutes", 50.0 + (day + carrier.hashCode()) % 100);
        rows.add(row);
      }
    }
    buildAndUploadSegment(mvTableConfig, mvSchema, rows, MV_COST_TABLE_NAME, "mvCostSeg");

    waitForAnyDocLoaded(MV_COST_TABLE_NAME, 60_000L);

    String definedSql = "SELECT " + TIME_COLUMN + ", Carrier, "
        + "SUM(ArrDelayMinutes) AS sum_ArrDelayMinutes "
        + "FROM " + SOURCE_TABLE_NAME + " "
        + "GROUP BY " + TIME_COLUMN + ", Carrier";
    MvDefinitionMetadata definition = new MvDefinitionMetadata(
        MV_COST_TABLE_OFFLINE,
        Collections.singletonList(SOURCE_TABLE_NAME),
        definedSql,
        Collections.emptyMap(),
        null);
    MvDefinitionMetadataUtils.persist(_propertyStore, definition, -1);

    MvRuntimeMetadata runtime = new MvRuntimeMetadata(
        MV_COST_TABLE_OFFLINE, DATA_MAX_TIME_MS, DATA_MAX_TIME_MS,
        MvFreshness.FRESH, new HashMap<>());
    MvRuntimeMetadataUtils.persist(_propertyStore, runtime, -1);
  }

  // -----------------------------------------------------------------------
  //  Segment building helpers
  // -----------------------------------------------------------------------

  private void buildAndUploadSegment(TableConfig tableConfig, Schema schema,
      List<GenericRow> rows, String tableName, String segmentName)
      throws Exception {
    File segOutputDir = new File(_tempDir, segmentName + "_output");
    File tarDir = new File(_tempDir, segmentName + "_tar");
    TestUtils.ensureDirectoriesExistAndEmpty(segOutputDir, tarDir);

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setTableName(tableConfig.getTableName());
    config.setOutDir(segOutputDir.getAbsolutePath());
    config.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    File segmentDir = new File(segOutputDir, segmentName);
    File tarFile = new File(tarDir, segmentName + TarCompressionUtils.TAR_GZ_FILE_EXTENSION);
    TarCompressionUtils.createCompressedTarFile(segmentDir, tarFile);

    uploadSegments(tableName, tarDir);
  }

  // -----------------------------------------------------------------------
  //  Utility helpers
  // -----------------------------------------------------------------------

  @Nullable
  private static String getHitMv(JsonNode response) {
    return response.has("hitMv") && !response.get("hitMv").isNull()
        ? response.get("hitMv").asText() : null;
  }

  private static void assertNoExceptions(JsonNode response) {
    JsonNode exceptions = response.get("exceptions");
    assertTrue(exceptions == null || exceptions.isEmpty(),
        "Query returned exceptions: " + exceptions);
  }

  private static boolean containsCandidate(JsonNode response, String mvTableNameWithType) {
    if (!response.has("candidateMvs")) {
      return false;
    }
    JsonNode candidates = response.get("candidateMvs");
    for (int i = 0; i < candidates.size(); i++) {
      if (mvTableNameWithType.equals(candidates.get(i).asText())) {
        return true;
      }
    }
    return false;
  }

  private void cleanupMvMetadata(String mvTableNameWithType) {
    try {
      MvDefinitionMetadataUtils.delete(_propertyStore, mvTableNameWithType);
    } catch (Exception e) {
      // Ignore cleanup failures
    }
    try {
      MvRuntimeMetadataUtils.delete(_propertyStore, mvTableNameWithType);
    } catch (Exception e) {
      // Ignore cleanup failures
    }
  }
}
