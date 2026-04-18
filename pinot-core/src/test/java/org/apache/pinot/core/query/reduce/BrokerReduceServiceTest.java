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
package org.apache.pinot.core.query.reduce;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.ObjectSerDeUtils;
import org.apache.pinot.core.common.datatable.DataTableBuilder;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.query.QueryThreadContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class BrokerReduceServiceTest {

  @Test
  public void testReduceTimeout()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable GROUP BY col1");
    DataSchema dataSchema =
        new DataSchema(new String[]{"col1", "count(*)"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    int numGroups = 5000;
    for (int i = 0; i < numGroups; i++) {
      dataTableBuilder.startRow();
      dataTableBuilder.setColumn(0, i);
      dataTableBuilder.setColumn(1, 1L);
      dataTableBuilder.finishRow();
    }
    DataTable dataTable = dataTableBuilder.build();
    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    int numInstances = 1000;
    for (int i = 0; i < numInstances; i++) {
      ServerRoutingInstance instance = new ServerRoutingInstance("localhost", i, TableType.OFFLINE);
      dataTableMap.put(instance, dataTable);
    }
    BrokerResponseNative brokerResponse = reduce(brokerReduceService, brokerRequest, dataTableMap, 1L);
    brokerReduceService.shutDown();

    List<QueryProcessingException> exceptions = brokerResponse.getExceptions();
    assertEquals(exceptions.size(), 1);
    assertEquals(exceptions.get(0).getErrorCode(), QueryErrorCode.BROKER_TIMEOUT.getId());
  }

  @Test
  public void testIgnoreMissingSegmentsFiltering() {
    // Build a simple broker reduce service
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));

    // Prepare a broker request with queryOptions toggled
    BrokerRequest brokerRequestNoIgnore = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable");
    BrokerRequest brokerRequestIgnore = CalciteSqlCompiler.compileToBrokerRequest("SELECT COUNT(*) FROM testTable");
    brokerRequestIgnore.getPinotQuery()
        .putToQueryOptions(CommonConstants.Broker.Request.QueryOptionKey.IGNORE_MISSING_SEGMENTS, "true");

    // Create a metadata-only DataTable with a SERVER_SEGMENT_MISSING exception
    DataTableBuilder dataTableBuilder = DataTableBuilderFactory.getDataTableBuilder(
        new DataSchema(new String[]{"count(*)"}, new ColumnDataType[]{ColumnDataType.LONG}));
    // no rows; build data table and then mark it metadata-only
    DataTable dataTable = dataTableBuilder.build().toMetadataOnlyDataTable();
    dataTable.addException(QueryErrorCode.SERVER_SEGMENT_MISSING,
        "1 segments [segA] missing on server: Server_localhost_12345");

    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerRoutingInstance("localhost", 12345, TableType.OFFLINE), dataTable);

    // Case 1: ignoreMissingSegments=false (default) -> exception should be present
    BrokerResponseNative responseNoIgnore = reduce(brokerReduceService, brokerRequestNoIgnore, dataTableMap, 10_000L);
    long missingErrCountNoIgnore = responseNoIgnore.getExceptions()
        .stream()
        .filter(e -> e.getErrorCode() == QueryErrorCode.SERVER_SEGMENT_MISSING.getId())
        .count();
    assertEquals(missingErrCountNoIgnore, 1L);

    // Case 2: ignoreMissingSegments=true -> exception should be filtered out
    BrokerResponseNative responseIgnore = reduce(brokerReduceService, brokerRequestIgnore, dataTableMap, 10_000L);
    long missingErrCountIgnore = responseIgnore.getExceptions()
        .stream()
        .filter(e -> e.getErrorCode() == QueryErrorCode.SERVER_SEGMENT_MISSING.getId())
        .count();
    assertEquals(missingErrCountIgnore, 0L);

    brokerReduceService.shutDown();
  }

  @Test
  public void testSelectionRowMergeFromTwoSources()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest =
        CalciteSqlCompiler.compileToBrokerRequest("SELECT col1, ts FROM testTable LIMIT 100");

    DataSchema dataSchema = new DataSchema(
        new String[]{"col1", "ts"},
        new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});

    // MV DataTable: 3 historical rows (time <= boundary)
    DataTableBuilder mvBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    for (int i = 1; i <= 3; i++) {
      mvBuilder.startRow();
      mvBuilder.setColumn(0, i);
      mvBuilder.setColumn(1, (long) i * 10);
      mvBuilder.finishRow();
    }
    DataTable mvDataTable = mvBuilder.build();

    // Base DataTable: 2 recent rows (time > boundary)
    DataTableBuilder baseBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    for (int i = 4; i <= 5; i++) {
      baseBuilder.startRow();
      baseBuilder.setColumn(0, i);
      baseBuilder.setColumn(1, (long) i * 10);
      baseBuilder.finishRow();
    }
    DataTable baseDataTable = baseBuilder.build();

    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerRoutingInstance("mv-host", 1234, TableType.OFFLINE), mvDataTable);
    dataTableMap.put(new ServerRoutingInstance("base-host", 5678, TableType.REALTIME), baseDataTable);

    BrokerResponseNative response = reduce(brokerReduceService, brokerRequest, dataTableMap, 10_000L);
    brokerReduceService.shutDown();

    assertTrue(response.getExceptions().isEmpty());
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable);
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 5);

    Set<Integer> col1Values = new HashSet<>();
    for (Object[] row : rows) {
      col1Values.add((Integer) row[0]);
    }
    assertEquals(col1Values, Set.of(1, 2, 3, 4, 5));
  }

  @Test
  public void testAggregationMergeFromTwoSources()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT SUM(amount), COUNT(*), MIN(amount), MAX(amount) FROM testTable");

    // Intermediate result types: SUM->DOUBLE, COUNT->LONG, MIN->DOUBLE, MAX->DOUBLE
    DataSchema dataSchema = new DataSchema(
        new String[]{"sum(amount)", "count(*)", "min(amount)", "max(amount)"},
        new ColumnDataType[]{ColumnDataType.DOUBLE, ColumnDataType.LONG,
            ColumnDataType.DOUBLE, ColumnDataType.DOUBLE});

    // MV DataTable: 1 row of pre-aggregated intermediate result (historical data)
    DataTableBuilder mvBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    mvBuilder.startRow();
    mvBuilder.setColumn(0, 1000.0);  // SUM
    mvBuilder.setColumn(1, 10L);     // COUNT
    mvBuilder.setColumn(2, 5.0);     // MIN
    mvBuilder.setColumn(3, 200.0);   // MAX
    mvBuilder.finishRow();
    DataTable mvDataTable = mvBuilder.build();

    // Base DataTable: 1 row of pre-aggregated intermediate result (recent data)
    DataTableBuilder baseBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    baseBuilder.startRow();
    baseBuilder.setColumn(0, 500.0);  // SUM
    baseBuilder.setColumn(1, 5L);     // COUNT
    baseBuilder.setColumn(2, 3.0);    // MIN
    baseBuilder.setColumn(3, 150.0);  // MAX
    baseBuilder.finishRow();
    DataTable baseDataTable = baseBuilder.build();

    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerRoutingInstance("mv-host", 1234, TableType.OFFLINE), mvDataTable);
    dataTableMap.put(new ServerRoutingInstance("base-host", 5678, TableType.REALTIME), baseDataTable);

    BrokerResponseNative response = reduce(brokerReduceService, brokerRequest, dataTableMap, 10_000L);
    brokerReduceService.shutDown();

    assertTrue(response.getExceptions().isEmpty());
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable);
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);

    Object[] row = rows.get(0);
    assertEquals(row[0], 1500.0);  // SUM: 1000 + 500
    assertEquals(row[1], 15L);     // COUNT: 10 + 5
    assertEquals(row[2], 3.0);     // MIN: min(5, 3)
    assertEquals(row[3], 200.0);   // MAX: max(200, 150)
  }

  @Test
  public void testDistinctCountHllMergeFromTwoSources()
      throws Exception {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT DISTINCTCOUNTHLL(userId) FROM testTable");

    DataSchema dataSchema = new DataSchema(
        new String[]{"distinctcounthll(userId)"},
        new ColumnDataType[]{ColumnDataType.OBJECT});

    // MV side: HLL sketch containing userIds {1, 2, 3, 4, 5}
    HyperLogLog mvHll = new HyperLogLog(CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M);
    for (int i = 1; i <= 5; i++) {
      mvHll.offer(i);
    }
    byte[] mvHllBytes = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(mvHll);
    DataTableBuilder mvBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    mvBuilder.startRow();
    mvBuilder.setColumn(0, new AggregationFunction.SerializedIntermediateResult(
        ObjectSerDeUtils.ObjectType.HyperLogLog.getValue(), mvHllBytes));
    mvBuilder.finishRow();
    DataTable mvDataTable = mvBuilder.build();

    // Base side: HLL sketch containing userIds {4, 5, 6, 7, 8}
    // 4 and 5 overlap with MV side — HLL union should deduplicate
    HyperLogLog baseHll = new HyperLogLog(CommonConstants.Helix.DEFAULT_HYPERLOGLOG_LOG2M);
    for (int i = 4; i <= 8; i++) {
      baseHll.offer(i);
    }
    byte[] baseHllBytes = ObjectSerDeUtils.HYPER_LOG_LOG_SER_DE.serialize(baseHll);
    DataTableBuilder baseBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    baseBuilder.startRow();
    baseBuilder.setColumn(0, new AggregationFunction.SerializedIntermediateResult(
        ObjectSerDeUtils.ObjectType.HyperLogLog.getValue(), baseHllBytes));
    baseBuilder.finishRow();
    DataTable baseDataTable = baseBuilder.build();

    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerRoutingInstance("mv-host", 1234, TableType.OFFLINE), mvDataTable);
    dataTableMap.put(new ServerRoutingInstance("base-host", 5678, TableType.REALTIME), baseDataTable);

    BrokerResponseNative response = reduce(brokerReduceService, brokerRequest, dataTableMap, 10_000L);
    brokerReduceService.shutDown();

    assertTrue(response.getExceptions().isEmpty());
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable);
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 1);

    // HLL merge does union: {1,2,3,4,5} ∪ {4,5,6,7,8} = {1,2,3,4,5,6,7,8}
    // For 8 distinct values with default log2m=8, HLL should return exact count
    long distinctCount = (Long) rows.get(0)[0];
    assertEquals(distinctCount, 8L);
  }

  @Test
  public void testGroupByMergeFromTwoSources()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT city, SUM(amount), COUNT(*) FROM testTable GROUP BY city");

    DataSchema dataSchema = new DataSchema(
        new String[]{"city", "sum(amount)", "count(*)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.DOUBLE, ColumnDataType.LONG});

    // MV DataTable: 3 groups from historical data (ts <= boundary)
    DataTableBuilder mvBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    mvBuilder.startRow();
    mvBuilder.setColumn(0, "BJ");
    mvBuilder.setColumn(1, 1000.0);
    mvBuilder.setColumn(2, 10L);
    mvBuilder.finishRow();
    mvBuilder.startRow();
    mvBuilder.setColumn(0, "SH");
    mvBuilder.setColumn(1, 2000.0);
    mvBuilder.setColumn(2, 20L);
    mvBuilder.finishRow();
    mvBuilder.startRow();
    mvBuilder.setColumn(0, "GZ");
    mvBuilder.setColumn(1, 500.0);
    mvBuilder.setColumn(2, 5L);
    mvBuilder.finishRow();
    DataTable mvDataTable = mvBuilder.build();

    // Base DataTable: 3 groups from recent data (ts > boundary)
    // BJ and SH overlap with MV; SZ is new
    DataTableBuilder baseBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    baseBuilder.startRow();
    baseBuilder.setColumn(0, "BJ");
    baseBuilder.setColumn(1, 300.0);
    baseBuilder.setColumn(2, 3L);
    baseBuilder.finishRow();
    baseBuilder.startRow();
    baseBuilder.setColumn(0, "SH");
    baseBuilder.setColumn(1, 400.0);
    baseBuilder.setColumn(2, 4L);
    baseBuilder.finishRow();
    baseBuilder.startRow();
    baseBuilder.setColumn(0, "SZ");
    baseBuilder.setColumn(1, 100.0);
    baseBuilder.setColumn(2, 1L);
    baseBuilder.finishRow();
    DataTable baseDataTable = baseBuilder.build();

    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerRoutingInstance("mv-host", 1234, TableType.OFFLINE), mvDataTable);
    dataTableMap.put(new ServerRoutingInstance("base-host", 5678, TableType.REALTIME), baseDataTable);

    BrokerResponseNative response = reduce(brokerReduceService, brokerRequest, dataTableMap, 10_000L);
    brokerReduceService.shutDown();

    assertTrue(response.getExceptions().isEmpty());
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable);
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 4);

    Map<String, Object[]> resultMap = new HashMap<>();
    for (Object[] row : rows) {
      resultMap.put((String) row[0], row);
    }
    // BJ: merge from both sources
    assertEquals(resultMap.get("BJ")[1], 1300.0);
    assertEquals(resultMap.get("BJ")[2], 13L);
    // SH: merge from both sources
    assertEquals(resultMap.get("SH")[1], 2400.0);
    assertEquals(resultMap.get("SH")[2], 24L);
    // GZ: only in MV
    assertEquals(resultMap.get("GZ")[1], 500.0);
    assertEquals(resultMap.get("GZ")[2], 5L);
    // SZ: only in Base
    assertEquals(resultMap.get("SZ")[1], 100.0);
    assertEquals(resultMap.get("SZ")[2], 1L);
  }

  @Test
  public void testGroupByWithTimeColumnMergeFromTwoSources()
      throws IOException {
    BrokerReduceService brokerReduceService =
        new BrokerReduceService(new PinotConfiguration(Map.of(Broker.CONFIG_OF_MAX_REDUCE_THREADS_PER_QUERY, 2)));
    BrokerRequest brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(
        "SELECT city, dateCol, SUM(amount) FROM testTable GROUP BY city, dateCol");

    // Schema: [group-key: city STRING, group-key: dateCol INT, agg: sum(amount) DOUBLE]
    DataSchema dataSchema = new DataSchema(
        new String[]{"city", "dateCol", "sum(amount)"},
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT, ColumnDataType.DOUBLE});

    // MV DataTable: historical aggregated groups
    DataTableBuilder mvBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    mvBuilder.startRow();
    mvBuilder.setColumn(0, "BJ");
    mvBuilder.setColumn(1, 20240601);
    mvBuilder.setColumn(2, 500.0);
    mvBuilder.finishRow();
    mvBuilder.startRow();
    mvBuilder.setColumn(0, "BJ");
    mvBuilder.setColumn(1, 20240701);
    mvBuilder.setColumn(2, 300.0);
    mvBuilder.finishRow();
    mvBuilder.startRow();
    mvBuilder.setColumn(0, "SH");
    mvBuilder.setColumn(1, 20240601);
    mvBuilder.setColumn(2, 800.0);
    mvBuilder.finishRow();
    DataTable mvDataTable = mvBuilder.build();

    // Base DataTable: recent groups
    // (BJ, 20240701) overlaps with MV — should be merged
    // (BJ, 20240801) and (SZ, 20240801) are new — should be preserved as-is
    DataTableBuilder baseBuilder = DataTableBuilderFactory.getDataTableBuilder(dataSchema);
    baseBuilder.startRow();
    baseBuilder.setColumn(0, "BJ");
    baseBuilder.setColumn(1, 20240701);
    baseBuilder.setColumn(2, 200.0);
    baseBuilder.finishRow();
    baseBuilder.startRow();
    baseBuilder.setColumn(0, "BJ");
    baseBuilder.setColumn(1, 20240801);
    baseBuilder.setColumn(2, 400.0);
    baseBuilder.finishRow();
    baseBuilder.startRow();
    baseBuilder.setColumn(0, "SZ");
    baseBuilder.setColumn(1, 20240801);
    baseBuilder.setColumn(2, 150.0);
    baseBuilder.finishRow();
    DataTable baseDataTable = baseBuilder.build();

    Map<ServerRoutingInstance, DataTable> dataTableMap = new HashMap<>();
    dataTableMap.put(new ServerRoutingInstance("mv-host", 1234, TableType.OFFLINE), mvDataTable);
    dataTableMap.put(new ServerRoutingInstance("base-host", 5678, TableType.REALTIME), baseDataTable);

    BrokerResponseNative response = reduce(brokerReduceService, brokerRequest, dataTableMap, 10_000L);
    brokerReduceService.shutDown();

    assertTrue(response.getExceptions().isEmpty());
    ResultTable resultTable = response.getResultTable();
    assertNotNull(resultTable);
    List<Object[]> rows = resultTable.getRows();
    assertEquals(rows.size(), 5);

    Map<String, Object[]> resultMap = new HashMap<>();
    for (Object[] row : rows) {
      String compositeKey = row[0] + "_" + row[1];
      resultMap.put(compositeKey, row);
    }
    // (BJ, 20240601): only in MV
    assertEquals(resultMap.get("BJ_20240601")[2], 500.0);
    // (BJ, 20240701): merged from both sources — 300 + 200
    assertEquals(resultMap.get("BJ_20240701")[2], 500.0);
    // (BJ, 20240801): only in Base
    assertEquals(resultMap.get("BJ_20240801")[2], 400.0);
    // (SH, 20240601): only in MV
    assertEquals(resultMap.get("SH_20240601")[2], 800.0);
    // (SZ, 20240801): only in Base
    assertEquals(resultMap.get("SZ_20240801")[2], 150.0);
  }

  private BrokerResponseNative reduce(BrokerReduceService brokerReduceService, BrokerRequest brokerRequest,
      Map<ServerRoutingInstance, DataTable> dataTableMap, long reduceTimeoutMs) {
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      return brokerReduceService.reduceOnDataTable(brokerRequest, brokerRequest, dataTableMap, reduceTimeoutMs,
          mock(BrokerMetrics.class));
    }
  }
}
