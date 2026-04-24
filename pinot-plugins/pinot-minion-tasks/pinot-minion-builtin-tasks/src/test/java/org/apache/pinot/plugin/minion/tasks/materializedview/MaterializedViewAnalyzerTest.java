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

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.UpsertConfig;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class MaterializedViewAnalyzerTest {

  private static final String SOURCE_TABLE = "orders";
  private static final String SOURCE_TABLE_OFFLINE = "orders_OFFLINE";
  private static final String TIME_COLUMN = "DaysSinceEpoch";
  /** Appended to every test SQL that is expected to reach validations beyond the LIMIT check. */
  private static final String DEFAULT_LIMIT = " LIMIT 1000";

  private ClusterInfoAccessor _mockAccessor;
  private TableConfig _sourceTableConfig;
  private Schema _sourceSchema;

  @BeforeMethod
  public void setUp() {
    _mockAccessor = mock(ClusterInfoAccessor.class);

    _sourceTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(SOURCE_TABLE_OFFLINE)
        .setTimeColumnName(TIME_COLUMN)
        .build();

    _sourceSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addSingleValueDimension("status", FieldSpec.DataType.STRING)
        .addMetric("amount", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    when(_mockAccessor.getTableConfig(SOURCE_TABLE_OFFLINE)).thenReturn(_sourceTableConfig);
    when(_mockAccessor.getTableSchema(SOURCE_TABLE_OFFLINE)).thenReturn(_sourceSchema);
  }

  // -----------------------------------------------------------------------
  //  Happy path
  // -----------------------------------------------------------------------

  @Test
  public void testValidSqlWithMatchingSchema() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt, sum(amount) AS total_amount "
        + "FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addMetric("total_amount", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = buildMvTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getSourceTableName(), SOURCE_TABLE);
    assertTrue(result.getSelectFields().contains("city"));
    assertTrue(result.getSelectFields().contains("cnt"));
    assertTrue(result.getSelectFields().contains("total_amount"));
    assertTrue(result.getSelectFields().contains(TIME_COLUMN));
    assertEquals(result.getSelectFields().size(), 4);

    // Verify partitionExprMaps
    assertNotNull(result.getPartitionExprMaps());
    assertEquals(result.getPartitionExprMaps().size(), 1);
    assertEquals(result.getPartitionExprMaps().get(TIME_COLUMN), TIME_COLUMN);
  }

  @Test
  public void testValidSqlBareColumnsOnly() {
    String sql = "SELECT DaysSinceEpoch, city, status FROM orders";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addSingleValueDimension("status", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = buildMvTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getSelectFields().size(), 3);
    assertEquals(result.getPartitionExprMaps().get(TIME_COLUMN), TIME_COLUMN);
  }

  @Test
  public void testValidSqlWithTimeTransformFunction() {
    String sql = "SELECT dateTimeConvert(DaysSinceEpoch, '1:DAYS:EPOCH', '1:DAYS:EPOCH', '7:DAYS') "
        + "AS weekBucket, city, count(*) AS cnt FROM orders "
        + "GROUP BY dateTimeConvert(DaysSinceEpoch, '1:DAYS:EPOCH', '1:DAYS:EPOCH', '7:DAYS'), city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("weekBucket", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "7:DAYS")
        .build();

    // The MV renames the time column via dateTimeConvert, so segmentsConfig.timeColumnName must
    // point to the SELECT alias 'weekBucket' — not the inherited base name.
    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName("weekBucket")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getPartitionExprMaps().size(), 1);
    assertEquals(result.getPartitionExprMaps().get(
        "datetimeconvert(DaysSinceEpoch, '1:DAYS:EPOCH', '1:DAYS:EPOCH', '7:DAYS')"), "weekBucket");
  }

  @Test
  public void testTimeColumnMissingFromSelect() {
    String sql = "SELECT city, count(*) AS cnt FROM orders GROUP BY city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    expectError(sql, mvSchema, "is not produced by any SELECT expression");
  }

  @Test
  public void testTimeColumnMissingFromGroupBy() {
    // Calcite enforces that non-aggregated SELECT columns must appear in GROUP BY,
    // so this SQL fails at syntax validation with Calcite's own error message.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    expectError(sql, mvSchema, "functionally dependent");
  }

  // -----------------------------------------------------------------------
  //  Step 1: SQL syntax errors
  // -----------------------------------------------------------------------

  @Test
  public void testInvalidSqlSyntax() {
    String sql = "SELCT city FROM orders";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .build();

    expectError(sql, mvSchema, "Invalid SQL syntax");
  }

  @Test
  public void testNullSql() {
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .build();

    expectError(null, mvSchema, "definedSQL must be specified");
  }

  @Test
  public void testEmptySql() {
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .build();

    expectError("", mvSchema, "definedSQL must be specified");
  }

  // -----------------------------------------------------------------------
  //  Step 1b: Explicit LIMIT requirement
  // -----------------------------------------------------------------------

  @Test
  public void testMissingLimitRejected() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    expectErrorRaw(sql, mvSchema, taskConfigs, "must specify an explicit LIMIT");
  }

  @Test
  public void testLimitAtMaxAllowed() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders "
        + "GROUP BY DaysSinceEpoch, city "
        + "LIMIT " + MaterializedViewTask.MAX_MV_QUERY_LIMIT;
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    TableConfig mvTableConfig = buildMvTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    // Must not throw — at-the-max is allowed.
    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(sql, mvTableConfig, mvSchema, taskConfigs, _mockAccessor);
    assertNotNull(result);
  }

  @Test
  public void testExtractDeclaredLimit() {
    String sql = "SELECT DaysSinceEpoch, city FROM orders LIMIT 2500";
    assertEquals(MaterializedViewAnalyzer.extractDeclaredLimit(sql), 2500);
  }

  @Test
  public void testExtractDeclaredLimitMissingThrows() {
    String sql = "SELECT DaysSinceEpoch, city FROM orders";
    try {
      MaterializedViewAnalyzer.extractDeclaredLimit(sql);
      fail("Expected IllegalStateException for SQL without LIMIT");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("must specify an explicit LIMIT"),
          "Unexpected message: " + e.getMessage());
    }
  }

  // -----------------------------------------------------------------------
  //  Step 2: Source table validation
  // -----------------------------------------------------------------------

  @Test
  public void testSourceTableNotFound() {
    String sql = "SELECT DaysSinceEpoch, city FROM nonexistent_table GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    when(_mockAccessor.getTableConfig("nonexistent_table_OFFLINE")).thenReturn(null);
    when(_mockAccessor.getTableConfig("nonexistent_table_REALTIME")).thenReturn(null);

    expectError(sql, mvSchema, "does not exist");
  }

  @Test
  public void testSourceTableNoTimeColumn() {
    TableConfig noTimeConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("no_time_table_OFFLINE")
        .build();
    when(_mockAccessor.getTableConfig("no_time_table_OFFLINE")).thenReturn(noTimeConfig);

    String sql = "SELECT DaysSinceEpoch, city FROM no_time_table GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    expectError(sql, mvSchema, "has no time column configured");
  }

  @Test
  public void testSourceTableNoDateTimeFieldSpec() {
    TableConfig withTimeConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("missing_spec_table_OFFLINE")
        .setTimeColumnName("missingCol")
        .build();
    Schema schemaWithoutSpec = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .build();
    when(_mockAccessor.getTableConfig("missing_spec_table_OFFLINE")).thenReturn(withTimeConfig);
    when(_mockAccessor.getTableSchema("missing_spec_table_OFFLINE")).thenReturn(schemaWithoutSpec);

    String sql = "SELECT DaysSinceEpoch, city FROM missing_spec_table GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    expectError(sql, mvSchema, "No DateTimeFieldSpec found");
  }

  // -----------------------------------------------------------------------
  //  Source-table type eligibility (Step 2): MV's coverage model assumes the base table is
  //  append-only with monotonically advancing time. Tables whose contents can be replaced or
  //  rewritten silently — upsert, dedup, dimension, REFRESH-push — must be rejected at create
  //  time so a known-broken MV cannot land in cluster metadata.
  // -----------------------------------------------------------------------

  @Test
  public void testRejectsUpsertSourceTable() {
    // Upsert: in-place row replacement breaks the assumption that a VALID time partition is
    // immutable; a late update to a covered interval would silently diverge from the MV.
    String mutableTable = "orders_upsert";
    TableConfig upsertCfg = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(mutableTable)
        .setTimeColumnName(TIME_COLUMN)
        .setUpsertConfig(new UpsertConfig(UpsertConfig.Mode.FULL))
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    when(_mockAccessor.getTableConfig(mutableTable + "_OFFLINE")).thenReturn(null);
    when(_mockAccessor.getTableConfig(mutableTable + "_REALTIME")).thenReturn(upsertCfg);
    when(_mockAccessor.getTableSchema(mutableTable + "_REALTIME")).thenReturn(schema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + mutableTable + " GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    expectError(sql, mvSchema, "upsert enabled");
  }

  @Test
  public void testRejectsDedupSourceTable() {
    // Dedup: the de-duplicated view is server-managed and not stable across reloads/TTLs;
    // MV would aggregate over a snapshot that the runtime can later disagree with.
    String dedupTable = "orders_dedup";
    TableConfig dedupCfg = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(dedupTable)
        .setTimeColumnName(TIME_COLUMN)
        .setDedupConfig(new DedupConfig(true, null))
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    when(_mockAccessor.getTableConfig(dedupTable + "_OFFLINE")).thenReturn(null);
    when(_mockAccessor.getTableConfig(dedupTable + "_REALTIME")).thenReturn(dedupCfg);
    when(_mockAccessor.getTableSchema(dedupTable + "_REALTIME")).thenReturn(schema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + dedupTable + " GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    expectError(sql, mvSchema, "dedup enabled");
  }

  @Test
  public void testRejectsDimensionSourceTable() {
    // Dimension table: fully replaced on every refresh and has no monotonic time concept;
    // the MV's coverageUpperMs model is meaningless here.
    String dimTable = "dim_lookup";
    TableConfig dimCfg = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(dimTable)
        .setTimeColumnName(TIME_COLUMN)
        .setIsDimTable(true)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    when(_mockAccessor.getTableConfig(dimTable + "_OFFLINE")).thenReturn(dimCfg);
    when(_mockAccessor.getTableSchema(dimTable + "_OFFLINE")).thenReturn(schema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + dimTable + " GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    expectError(sql, mvSchema, "dimension table");
  }

  @Test
  public void testRejectsRefreshPushTable() {
    // REFRESH push: each push wholesale replaces base segments, so any MV partition already
    // marked VALID becomes immediately suspect after the next push.
    String refreshTable = "orders_refresh";
    IngestionConfig ingestionCfg = new IngestionConfig();
    ingestionCfg.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY"));
    TableConfig refreshCfg = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(refreshTable)
        .setTimeColumnName(TIME_COLUMN)
        .setIngestionConfig(ingestionCfg)
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    when(_mockAccessor.getTableConfig(refreshTable + "_OFFLINE")).thenReturn(refreshCfg);
    when(_mockAccessor.getTableSchema(refreshTable + "_OFFLINE")).thenReturn(schema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + refreshTable + " GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    expectError(sql, mvSchema, "REFRESH push type");
  }

  @Test
  public void testRejectsRefreshPushTableViaLegacyField() {
    // Legacy: REFRESH was set via the deprecated SegmentsValidationAndRetentionConfig field.
    // resolveSegmentPushType must fall through to it so older configs cannot bypass the guard.
    String refreshTable = "orders_refresh_legacy";
    TableConfig refreshCfg = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(refreshTable)
        .setTimeColumnName(TIME_COLUMN)
        .setSegmentPushType("REFRESH")
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    when(_mockAccessor.getTableConfig(refreshTable + "_OFFLINE")).thenReturn(refreshCfg);
    when(_mockAccessor.getTableSchema(refreshTable + "_OFFLINE")).thenReturn(schema);

    String sql = "SELECT DaysSinceEpoch, city FROM " + refreshTable + " GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();
    expectError(sql, mvSchema, "REFRESH push type");
  }

  @Test
  public void testSourceColumnNotExist() {
    String sql = "SELECT DaysSinceEpoch, city, sum(nonexistent_col) AS total FROM orders "
        + "GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("total", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    expectError(sql, mvSchema, "does not exist in source table");
  }

  // -----------------------------------------------------------------------
  //  Step 3: MV schema column validation
  // -----------------------------------------------------------------------

  @Test
  public void testMvSchemaColumnNotCoveredBySelect() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addMetric("extra_column", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    expectError(sql, mvSchema, "is not produced by any SELECT expression");
  }

  @Test
  public void testSelectFieldNotInMvSchema() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt, sum(amount) AS total "
        + "FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    expectError(sql, mvSchema, "does not match any column in the MV table schema");
  }

  @Test
  public void testAggregateWithoutAlias() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    expectError(sql, mvSchema, "must have an AS alias");
  }

  // -----------------------------------------------------------------------
  //  Step 4: Task config parameter validation
  // -----------------------------------------------------------------------

  @Test
  public void testNonOfflineTableType() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig realtimeConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName("mv_orders")
        .setTimeColumnName(TIME_COLUMN)
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), realtimeConfig, mvSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for non-OFFLINE table");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("only supports OFFLINE"), "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testInvalidBucketTimePeriod() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    taskConfigs.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "not_a_period");

    expectError(sql, mvSchema, taskConfigs, "Invalid bucketTimePeriod");
  }

  @Test
  public void testInvalidMaxNumRecordsPerSegment() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    taskConfigs.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "-5");

    expectError(sql, mvSchema, taskConfigs, "maxNumRecordsPerSegment must be positive");
  }

  @Test
  public void testNonNumericMaxNumRecordsPerSegment() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    Map<String, String> taskConfigs = buildTaskConfigs(sql);
    taskConfigs.put(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY, "abc");

    expectError(sql, mvSchema, taskConfigs, "Invalid maxNumRecordsPerSegment");
  }

  // -----------------------------------------------------------------------
  //  Complex SQL
  // -----------------------------------------------------------------------

  @Test
  public void testComplexSqlWithMultipleAggregations() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt, sum(amount) AS total, "
        + "min(amount) AS min_amt, max(amount) AS max_amt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addMetric("total", FieldSpec.DataType.DOUBLE)
        .addMetric("min_amt", FieldSpec.DataType.DOUBLE)
        .addMetric("max_amt", FieldSpec.DataType.DOUBLE)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = buildMvTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getSelectFields().size(), 6);
  }

  @Test
  public void testRealtimeSourceTable() {
    String realtimeTable = "rt_orders_REALTIME";
    TableConfig rtConfig = new TableConfigBuilder(TableType.REALTIME)
        .setTableName(realtimeTable)
        .setTimeColumnName(TIME_COLUMN)
        .build();
    Schema rtSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    when(_mockAccessor.getTableConfig("rt_orders_OFFLINE")).thenReturn(null);
    when(_mockAccessor.getTableConfig(realtimeTable)).thenReturn(rtConfig);
    when(_mockAccessor.getTableSchema(realtimeTable)).thenReturn(rtSchema);

    String sql = "SELECT DaysSinceEpoch, city FROM rt_orders";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = buildMvTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getSourceTableName(), "rt_orders");
  }

  // -----------------------------------------------------------------------
  //  Step 6: MV time-column alignment (segmentsConfig.timeColumnName)
  // -----------------------------------------------------------------------

  @Test
  public void testRejectsWhenMvTimeColumnMissing() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for unset MV timeColumnName");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("segmentsConfig.timeColumnName must be set"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testRejectsWhenMvTimeColumnNotInSchema() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    // timeColumnName points to a column that doesn't exist in the MV schema at all.
    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName("nonexistent_time_col")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for MV timeColumnName missing from schema");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("MV time column 'nonexistent_time_col' does not exist in MV schema"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testRejectsWhenMvTimeColumnIsNotDateTime() {
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    // timeColumnName points to a plain dimension, not a registered dateTime column.
    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName("city")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for MV timeColumnName not being a dateTime column");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("is not a dateTime field in the MV schema"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testRejectsWhenMvTimeColumnNotProducedBySelect() {
    // Simulates the real-world misconfig: base table time column is DaysSinceEpoch; the
    // definedSql transforms it via date_trunc into a coarser 'day' column; but the MV
    // TableConfig inherited timeColumnName=DaysSinceEpoch from the base table without
    // updating it. The MV will not physically contain DaysSinceEpoch.
    String sql = "SELECT date_trunc('DAY', DaysSinceEpoch) AS day, city, count(*) AS cnt "
        + "FROM orders GROUP BY day, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("day", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName(TIME_COLUMN)
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for MV timeColumnName not produced by SELECT");
    } catch (IllegalStateException e) {
      // The column is absent from the MV schema entirely, so invariant (b) fires first
      // with a message that still points the user to the root cause.
      assertTrue(e.getMessage().contains("MV time column '" + TIME_COLUMN + "' does not exist in MV schema"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testAcceptsWhenMvTimeColumnIsSelectAlias() {
    // Happy path mirroring the previous test but with timeColumnName correctly set to
    // the SELECT-produced alias 'day'. Note: `inputTimeUnit='DAYS'` is passed explicitly so
    // Step-7 inference matches the base column unit (Pinot's date_trunc defaults
    // inputTimeUnit to MILLISECONDS, NOT to the base unit). Granularity is 1:DAYS because
    // DateTruncRule maps the truncation unit 'DAY' to its TimeUnit equivalent DAYS — the
    // MV granularity field is parsed via TimeUnit.valueOf and only accepts plural names.
    String sql = "SELECT date_trunc('DAY', DaysSinceEpoch, 'DAYS') AS day, city, count(*) AS cnt "
        + "FROM orders GROUP BY day, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("day", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName("day")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getPartitionExprMaps().size(), 1);
    assertTrue(result.getPartitionExprMaps().containsValue("day"),
        "Expected partitionExprMaps to map some base-table expression -> 'day', got: "
            + result.getPartitionExprMaps());
  }

  // -----------------------------------------------------------------------
  //  Step 7: MV time column format / granularity inference (TimeExprInferrer)
  //
  //  Each rule in the inferrer (identity, dateTimeConvert, date_trunc, toDateTime)
  //  is exercised here through analyze() end-to-end with at least one happy and one
  //  mismatch case. The famous "date_trunc + 1:DAYS:EPOCH" misconception gets its own
  //  test because users routinely conflate the storage-unit format with the truncation
  //  granularity, and the Step-7 reason string is what makes the error self-explanatory.
  // -----------------------------------------------------------------------

  @Test
  public void testStep7IdentityFormatMismatch() {
    // Bare identifier: the identity rule requires MV format/granularity to equal base
    // verbatim. Here MV declares 1:HOURS:EPOCH while base is 1:DAYS:EPOCH — would silently
    // mis-bucket every time-range filter at query time without this guard.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt "
        + "FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:DAYS")
        .build();

    expectError(sql, mvSchema, "format mismatch");
  }

  @Test
  public void testStep7IdentityGranularityMismatch() {
    // Identity again, format matches but granularity doesn't.
    String sql = "SELECT DaysSinceEpoch, city, count(*) AS cnt "
        + "FROM orders GROUP BY DaysSinceEpoch, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "7:DAYS")
        .build();

    expectError(sql, mvSchema, "granularity mismatch");
  }

  @Test
  public void testStep7DateTimeConvertOutputFormatMismatch() {
    // dateTimeConvert outputFormat='1:DAYS:EPOCH' but MV declares '1:HOURS:EPOCH'.
    String sql = "SELECT dateTimeConvert(DaysSinceEpoch, '1:DAYS:EPOCH', '1:DAYS:EPOCH', '1:DAYS') "
        + "AS daily, city, count(*) AS cnt FROM orders "
        + "GROUP BY dateTimeConvert(DaysSinceEpoch, '1:DAYS:EPOCH', '1:DAYS:EPOCH', '1:DAYS'), city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("daily", FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName("daily")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    expectErrorRaw(withLimit(sql), mvSchema, mvTableConfig, taskConfigs, "format mismatch");
  }

  @Test
  public void testStep7DateTimeConvertGranularityMismatch() {
    // outputFormat matches MV but the declared granularity '7:DAYS' disagrees with MV '1:DAYS'.
    String sql = "SELECT dateTimeConvert(DaysSinceEpoch, '1:DAYS:EPOCH', '1:DAYS:EPOCH', '7:DAYS') "
        + "AS weekly, city, count(*) AS cnt FROM orders "
        + "GROUP BY dateTimeConvert(DaysSinceEpoch, '1:DAYS:EPOCH', '1:DAYS:EPOCH', '7:DAYS'), city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("weekly", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName("weekly")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    expectErrorRaw(withLimit(sql), mvSchema, mvTableConfig, taskConfigs, "granularity mismatch");
  }

  @Test
  public void testStep7DateTimeConvertInputFormatMismatchesBase() {
    // inputFormat='1:HOURS:EPOCH' lies about the base column (which is '1:DAYS:EPOCH'),
    // so even if outputFormat aligns with MV, the function will silently re-interpret the
    // base values. Step-7 catches this at definition time.
    String sql = "SELECT dateTimeConvert(DaysSinceEpoch, '1:HOURS:EPOCH', '1:DAYS:EPOCH', '1:DAYS') "
        + "AS daily, city, count(*) AS cnt FROM orders "
        + "GROUP BY dateTimeConvert(DaysSinceEpoch, '1:HOURS:EPOCH', '1:DAYS:EPOCH', '1:DAYS'), city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("daily", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName("daily")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    expectErrorRaw(withLimit(sql), mvSchema, mvTableConfig, taskConfigs,
        "dateTimeConvert inputFormat");
  }

  @Test
  public void testStep7DateTruncHappyOnMillisBase() {
    // Base column is in MILLISECONDS, so date_trunc's default inputTimeUnit (MILLISECONDS)
    // matches and the rule infers format=1:MILLISECONDS:EPOCH (storage unit), granularity=1:DAYS
    // (DateTruncRule maps the truncation unit 'DAY' to TimeUnit.DAYS).
    useMillisecondsBase();
    String sql = "SELECT date_trunc('DAY', ts) AS day, city, count(*) AS cnt "
        + "FROM orders_ms GROUP BY day, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("day", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders_ms")
        .setTimeColumnName("day")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertTrue(result.getPartitionExprMaps().containsValue("day"));
  }

  @Test
  public void testStep7DateTruncFormatMisconceptionDaysEpoch() {
    // Classic user mistake: date_trunc('DAY', ts) returns a long in the *storage* unit
    // (MILLISECONDS by default), NOT in days. Declaring the MV column as 1:DAYS:EPOCH would
    // silently divide every value by 86.4M at read time.
    useMillisecondsBase();
    String sql = "SELECT date_trunc('DAY', ts) AS day, city, count(*) AS cnt "
        + "FROM orders_ms GROUP BY day, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        // BUG: format says DAYS but date_trunc still returns a millis-since-epoch long.
        .addDateTime("day", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders_ms")
        .setTimeColumnName("day")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for date_trunc storage-unit misconception");
    } catch (IllegalStateException e) {
      String msg = e.getMessage();
      assertTrue(msg.contains("format mismatch"), "Unexpected message: " + msg);
      // Must show what was actually expected (storage unit) so the user can reconcile.
      assertTrue(msg.contains("1:MILLISECONDS:EPOCH"),
          "Expected message to surface inferred '1:MILLISECONDS:EPOCH', got: " + msg);
      // Reason text must explain the storage-vs-granularity gotcha.
      assertTrue(msg.contains("storage unit"),
          "Expected reason to mention storage unit, got: " + msg);
    }
  }

  @Test
  public void testStep7DateTruncDefaultInputUnitMismatchesBase() {
    // Base is in DAYS, but date_trunc with no explicit inputTimeUnit defaults to MILLISECONDS.
    // The rule rejects this because Pinot's runtime would silently reinterpret day-counts
    // as milliseconds-since-epoch.
    String sql = "SELECT date_trunc('DAY', DaysSinceEpoch) AS day, city, count(*) AS cnt "
        + "FROM orders GROUP BY day, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("day", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName("day")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    expectErrorRaw(withLimit(sql), mvSchema, mvTableConfig, taskConfigs,
        "date_trunc inputTimeUnit");
  }

  @Test
  public void testStep7ToDateTimeHappy() {
    // toDateTime renders to a SIMPLE_DATE_FORMAT string. Granularity isn't inferred but
    // must still be declared (any non-empty value is accepted in v1).
    useMillisecondsBase();
    String sql = "SELECT toDateTime(ts, 'yyyy-MM-dd') AS dayStr, city, count(*) AS cnt "
        + "FROM orders_ms GROUP BY dayStr, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("dayStr", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders_ms")
        .setTimeColumnName("dayStr")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertTrue(result.getPartitionExprMaps().containsValue("dayStr"));
  }

  @Test
  public void testStep7ToDateTimePatternMismatch() {
    // MV pattern 'yyyy-MM' doesn't match the function's 'yyyy-MM-dd'.
    useMillisecondsBase();
    String sql = "SELECT toDateTime(ts, 'yyyy-MM-dd') AS dayStr, city, count(*) AS cnt "
        + "FROM orders_ms GROUP BY dayStr, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("dayStr", FieldSpec.DataType.STRING, "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders_ms")
        .setTimeColumnName("dayStr")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    expectErrorRaw(withLimit(sql), mvSchema, mvTableConfig, taskConfigs, "format mismatch");
  }

  @Test
  public void testStep7ToDateTimeMvFormatNotSdf() {
    // toDateTime requires MV format to be SIMPLE_DATE_FORMAT-style; an EPOCH format here is
    // a structural mismatch caught by the SdfPatternFormatMatcher.
    useMillisecondsBase();
    String sql = "SELECT toDateTime(ts, 'yyyy-MM-dd') AS dayStr, city, count(*) AS cnt "
        + "FROM orders_ms GROUP BY dayStr, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("dayStr", FieldSpec.DataType.STRING, "1:MILLISECONDS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders_ms")
        .setTimeColumnName("dayStr")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    expectErrorRaw(withLimit(sql), mvSchema, mvTableConfig, taskConfigs,
        "SIMPLE_DATE_FORMAT");
  }

  @Test
  public void testStep7UnsupportedFunctionRejected() {
    // fromEpochDays is a real Pinot scalar but is intentionally not in the MV time-expr
    // whitelist (v1). The rejection message must enumerate the supported set so the user
    // can self-correct without consulting source code.
    String sql = "SELECT fromEpochDays(DaysSinceEpoch) AS ts_ms, city, count(*) AS cnt "
        + "FROM orders GROUP BY ts_ms, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("ts_ms", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName("ts_ms")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    try {
      MaterializedViewAnalyzer.analyze(withLimit(sql), mvTableConfig, mvSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException for unsupported time function");
    } catch (IllegalStateException e) {
      String msg = e.getMessage();
      assertTrue(msg.contains("unsupported function") || msg.contains("Supported"),
          "Expected message to flag unsupported function, got: " + msg);
      // Whitelist must be visible.
      assertTrue(msg.contains("datetimeconvert") && msg.contains("datetrunc") && msg.contains("todatetime"),
          "Expected supported-set hint in message, got: " + msg);
    }
  }

  @Test
  public void testStep7ArithmeticTimeExprRejected() {
    // ts / 86400 parses as divide(ts, 86400) — not in the time-function whitelist, so the
    // generic "unsupported function" path fires here too.
    useMillisecondsBase();
    String sql = "SELECT ts / 86400 AS dayBucket, city, count(*) AS cnt "
        + "FROM orders_ms GROUP BY dayBucket, city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("dayBucket", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders_ms")
        .setTimeColumnName("dayBucket")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    expectErrorRaw(withLimit(sql), mvSchema, mvTableConfig, taskConfigs, "unsupported function");
  }

  @Test
  public void testStep7NestedFunctionRejected() {
    // Nested call: date_trunc(...) is not a bare identifier, so dateTimeConvert's first-arg
    // check fires. v1 rejects nesting outright; users must inline a single transformation.
    useMillisecondsBase();
    String sql = "SELECT dateTimeConvert(date_trunc('DAY', ts), '1:MILLISECONDS:EPOCH', "
        + "'1:DAYS:EPOCH', '1:DAYS') AS day, city, count(*) AS cnt FROM orders_ms "
        + "GROUP BY dateTimeConvert(date_trunc('DAY', ts), '1:MILLISECONDS:EPOCH', "
        + "'1:DAYS:EPOCH', '1:DAYS'), city";
    Schema mvSchema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("cnt", FieldSpec.DataType.LONG)
        .addDateTime("day", FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS")
        .build();

    TableConfig mvTableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders_ms")
        .setTimeColumnName("day")
        .build();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    expectErrorRaw(withLimit(sql), mvSchema, mvTableConfig, taskConfigs,
        "first argument must be the base time column");
  }

  // -----------------------------------------------------------------------
  //  Helpers
  // -----------------------------------------------------------------------

  private TableConfig buildMvTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("mv_orders")
        .setTimeColumnName(TIME_COLUMN)
        .build();
  }

  private Map<String, String> buildTaskConfigs(String sql) {
    Map<String, String> taskConfigs = new HashMap<>();
    taskConfigs.put(MaterializedViewTask.DEFINED_SQL_KEY, sql);
    taskConfigs.put(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, "1d");
    return taskConfigs;
  }

  private void expectError(String sql, Schema mvSchema, String expectedMessageFragment) {
    expectError(sql, mvSchema, buildTaskConfigs(sql), expectedMessageFragment);
  }

  private void expectError(String sql, Schema mvSchema, Map<String, String> taskConfigs,
      String expectedMessageFragment) {
    expectErrorRaw(withLimit(sql), mvSchema, taskConfigs, expectedMessageFragment);
  }

  /**
   * Same as {@link #expectError(String, Schema, Map, String)} but does not append a default
   * LIMIT.  Used by tests that intentionally exercise the LIMIT-validation path.
   */
  private void expectErrorRaw(String sql, Schema mvSchema, Map<String, String> taskConfigs,
      String expectedMessageFragment) {
    expectErrorRaw(sql, mvSchema, buildMvTableConfig(), taskConfigs, expectedMessageFragment);
  }

  /**
   * Variant that lets the caller supply a custom MV {@link TableConfig} (e.g. with a
   * SELECT-alias time column name). Step-7 tests need this because the time column is
   * usually an alias of the base time column.
   */
  private void expectErrorRaw(String sql, Schema mvSchema, TableConfig mvTableConfig,
      Map<String, String> taskConfigs, String expectedMessageFragment) {
    try {
      MaterializedViewAnalyzer.analyze(sql, mvTableConfig, mvSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException containing: " + expectedMessageFragment);
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(expectedMessageFragment),
          "Expected message containing '" + expectedMessageFragment + "', got: " + e.getMessage());
    }
  }

  /**
   * Wires a MILLISECONDS-based source table {@code orders_ms} into the mock accessor.
   * Step-7 tests for {@code date_trunc} / {@code toDateTime} need a millis base because both
   * rules require a unitary EPOCH base and {@code date_trunc}'s default inputTimeUnit is
   * MILLISECONDS — using the default DAYS-based source would force every test to also pass
   * an explicit {@code inputTimeUnit}, obscuring what's actually being tested.
   */
  private void useMillisecondsBase() {
    String tableName = "orders_ms_OFFLINE";
    TableConfig cfg = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(tableName)
        .setTimeColumnName("ts")
        .build();
    Schema schema = new Schema.SchemaBuilder()
        .addSingleValueDimension("city", FieldSpec.DataType.STRING)
        .addMetric("amount", FieldSpec.DataType.DOUBLE)
        .addDateTime("ts", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();
    when(_mockAccessor.getTableConfig(tableName)).thenReturn(cfg);
    when(_mockAccessor.getTableSchema(tableName)).thenReturn(schema);
  }

  /** Returns {@code sql} as-is if it already ends with a LIMIT clause, otherwise appends one. */
  private static String withLimit(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    return sql.toUpperCase().contains(" LIMIT ") ? sql : sql + DEFAULT_LIMIT;
  }
}
