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
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
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
        MaterializedViewAnalyzer.analyze(sql, mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

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
        MaterializedViewAnalyzer.analyze(sql, mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

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

    TableConfig mvTableConfig = buildMvTableConfig();
    Map<String, String> taskConfigs = buildTaskConfigs(sql);

    MaterializedViewAnalyzer.AnalysisResult result =
        MaterializedViewAnalyzer.analyze(sql, mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

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
      MaterializedViewAnalyzer.analyze(sql, realtimeConfig, mvSchema, taskConfigs, _mockAccessor);
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
        MaterializedViewAnalyzer.analyze(sql, mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

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
        MaterializedViewAnalyzer.analyze(sql, mvTableConfig, mvSchema, taskConfigs, _mockAccessor);

    assertNotNull(result);
    assertEquals(result.getSourceTableName(), "rt_orders");
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
    TableConfig mvTableConfig = buildMvTableConfig();
    try {
      MaterializedViewAnalyzer.analyze(sql, mvTableConfig, mvSchema, taskConfigs, _mockAccessor);
      fail("Expected IllegalStateException containing: " + expectedMessageFragment);
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains(expectedMessageFragment),
          "Expected message containing '" + expectedMessageFragment + "', got: " + e.getMessage());
    }
  }
}
