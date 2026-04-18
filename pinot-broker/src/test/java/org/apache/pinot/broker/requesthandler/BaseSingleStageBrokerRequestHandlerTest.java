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
package org.apache.pinot.broker.requesthandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.broker.api.AccessControl;
import org.apache.pinot.broker.broker.AccessControlFactory;
import org.apache.pinot.broker.broker.AllowAllAccessControlFactory;
import org.apache.pinot.broker.materializedview.ExecutionMode;
import org.apache.pinot.broker.materializedview.MatchType;
import org.apache.pinot.broker.materializedview.MvQueryRewriteEngine;
import org.apache.pinot.broker.materializedview.MvRewritePlan;
import org.apache.pinot.broker.materializedview.MvRewriteResult;
import org.apache.pinot.broker.queryquota.QueryQuotaManager;
import org.apache.pinot.broker.routing.manager.BrokerRoutingManager;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.core.routing.RoutingTable;
import org.apache.pinot.core.routing.SegmentsToQuery;
import org.apache.pinot.core.routing.TableRouteInfo;
import org.apache.pinot.core.routing.timeboundary.TimeBoundaryInfo;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.accounting.ThreadAccountantUtils;
import org.apache.pinot.spi.auth.TableAuthorizationResult;
import org.apache.pinot.spi.auth.TableRowColAccessResultImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.eventlistener.query.BrokerQueryEventListenerFactory;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.apache.pinot.spi.trace.LoggerConstants;
import org.apache.pinot.spi.trace.RequestContext;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Query.Range;
import org.apache.pinot.sql.FilterKind;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.util.TestUtils;
import org.mockito.Mockito;
import org.slf4j.MDC;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class BaseSingleStageBrokerRequestHandlerTest {

  @AfterMethod
  public void cleanupMdc() {
    MDC.clear();
  }

  @Test
  public void testUpdateColumnNames() {
    String query = "SELECT database.my_table.column_name_1st, column_name_2nd from database.my_table";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    Map<String, String> columnNameMap =
        Map.of("column_name_1st", "column_name_1st", "column_name_2nd", "column_name_2nd");
    BaseSingleStageBrokerRequestHandler.updateColumnNames("database.my_table", pinotQuery, false, columnNameMap);
    Assert.assertEquals(pinotQuery.getSelectList().size(), 2);
    for (Expression expression : pinotQuery.getSelectList()) {
      String columnName = expression.getIdentifier().getName();
      if (columnName.endsWith("column_name_1st")) {
        Assert.assertEquals(columnName, "column_name_1st");
      } else if (columnName.endsWith("column_name_2nd")) {
        Assert.assertEquals(columnName, "column_name_2nd");
      } else {
        Assert.fail("rewritten column name should be column_name_1st or column_name_1st, but is " + columnName);
      }
    }
  }

  @Test
  public void testOnQueryCompletionHookReceivesBrokerResponse() {
    // Verify that the overridable onQueryCompletion(RequestContext, BrokerResponse) hook is invoked
    // and receives the BrokerResponse that handleRequest() produced.
    AtomicReference<BrokerResponse> capturedResponse = new AtomicReference<>();

    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);
    BrokerMetrics.register(mock(BrokerMetrics.class));
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);
    TableCache tableCache = mock(TableCache.class);

    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
            mock(org.apache.pinot.core.routing.RoutingManager.class), new AllowAllAccessControlFactory(),
            queryQuotaManager, tableCache, ThreadAccountantUtils.getNoOpAccountant(), null) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
              BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return new BrokerResponseNative();
          }

          @Override
          protected void onQueryCompletion(RequestContext requestContext, BrokerResponse brokerResponse) {
            capturedResponse.set(brokerResponse);
          }
        };

    try {
      handler.handleRequest("SELECT 1");
    } catch (Exception ignored) {
      // routing may fail — we only care that the hook was called with a non-null response
    }
    Assert.assertNotNull(capturedResponse.get(),
        "onQueryCompletion hook must be called with the BrokerResponse from handleRequest");
  }

  @Test
  public void testGetActualColumnNameCaseSensitive() {
    Map<String, String> columnNameMap = new HashMap<>();
    columnNameMap.put("student_name", "student_name");
    String actualColumnName =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable.student_name", columnNameMap,
            false);
    Assert.assertEquals(actualColumnName, "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.mytable", "db1.mytable.student_name",
            columnNameMap, false), "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.mytable", "mytable.student_name", columnNameMap,
            false), "student_name");
    boolean exceptionThrown = false;
    try {
      BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable2.student_name", columnNameMap, false);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    exceptionThrown = false;
    try {
      BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE.student_name", columnNameMap, false);
      Assert.fail("should throw exception if case sensitive and table name different");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("mytable_student_name", "mytable_student_name");
    String wrongColumnName2 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable_student_name", columnNameMap,
            false);
    Assert.assertEquals(wrongColumnName2, "mytable_student_name");

    columnNameMap.put("mytable", "mytable");
    String wrongColumnName3 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "mytable", columnNameMap, false);
    Assert.assertEquals(wrongColumnName3, "mytable");
  }

  @Test
  public void testGetActualColumnNameCaseInSensitive() {
    Map<String, String> columnNameMap = new HashMap<>();
    columnNameMap.put("student_name", "student_name");
    String actualColumnName =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE.student_name", columnNameMap, true);
    Assert.assertEquals(actualColumnName, "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.MYTABLE", "DB1.mytable.student_name",
            columnNameMap, true), "student_name");
    Assert.assertEquals(
        BaseSingleStageBrokerRequestHandler.getActualColumnName("db1.mytable", "MYTABLE.student_name", columnNameMap,
            true), "student_name");
    boolean exceptionThrown = false;
    try {
      BaseSingleStageBrokerRequestHandler.getActualColumnName("student", "MYTABLE2.student_name", columnNameMap, true);
      Assert.fail("should throw exception if column is not known");
    } catch (BadQueryRequestException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown, "should throw exception if column is not known");
    columnNameMap.put("mytable_student_name", "mytable_student_name");
    String wrongColumnName2 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("mytable", "MYTABLE_student_name", columnNameMap, true);
    Assert.assertEquals(wrongColumnName2, "mytable_student_name");

    columnNameMap.put("mytable", "mytable");
    String wrongColumnName3 =
        BaseSingleStageBrokerRequestHandler.getActualColumnName("MYTABLE", "mytable", columnNameMap, true);
    Assert.assertEquals(wrongColumnName3, "mytable");
  }

  @Test
  public void testCancelQuery() {
    String tableName = "myTable_OFFLINE";
    // Mock pretty much everything until the query can be submitted.
    TableCache tableCache = mock(TableCache.class);
    TableConfig tableCfg = mock(TableConfig.class);
    when(tableCache.getActualTableName(anyString())).thenReturn(tableName);
    TenantConfig tenant = new TenantConfig("tier_BROKER", "tier_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(tableName)).thenReturn(tableCfg);
    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(tableName)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(tableName)).thenReturn(10000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
        new SegmentsToQuery(List.of("segment01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);
    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);
    CountDownLatch latch = new CountDownLatch(1);
    long[] testRequestId = {-1};
    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);
    BaseSingleStageBrokerRequestHandler requestHandler =
        new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(), routingManager,
            new AllowAllAccessControlFactory(), queryQuotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null, null) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
              BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext)
              throws Exception {
            testRequestId[0] = requestId;
            latch.await();
            return null;
          }

          @Override
          protected BrokerResponseNative processMvSplitBrokerRequest(long requestId,
              BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute, TableRouteInfo mvRoute,
              long timeoutMs, ServerStats serverStats, RequestContext requestContext)
              throws Exception {
            throw new UnsupportedOperationException("Not implemented in test");
          }
        };
    CompletableFuture.runAsync(() -> {
      try {
        requestHandler.handleRequest(String.format("select * from %s limit 10", tableName));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    TestUtils.waitForCondition((aVoid) -> requestHandler.getRunningServers(testRequestId[0]).size() == 1, 500, 5000,
        "Failed to submit query");
    Map.Entry<Long, String> entry = requestHandler.getRunningQueries().entrySet().iterator().next();
    Assert.assertEquals(entry.getKey().longValue(), testRequestId[0]);
    Assert.assertTrue(entry.getValue().contains("select * from myTable_OFFLINE limit 10"));
    Set<ServerInstance> servers = requestHandler.getRunningServers(testRequestId[0]);
    Assert.assertEquals(servers.size(), 1);
    Assert.assertEquals(servers.iterator().next().getHostname(), "server01");
    Assert.assertEquals(servers.iterator().next().getPort(), 9000);
    Assert.assertEquals(servers.iterator().next().getInstanceId(), "server01_9000");
    Assert.assertEquals(servers.iterator().next().getAdminEndpoint(), "http://server01:8097");
    latch.countDown();
  }

  @Test
  public void testAddRoutingPolicyInErrMsg() {
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", null, null), "error1");
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", "rt_rp", null),
        "error1, with routing policy: rt_rp [realtime]");
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", null, "off_rp"),
        "error1, with routing policy: off_rp [offline]");
    Assert.assertEquals(BaseSingleStageBrokerRequestHandler.addRoutingPolicyInErrMsg("error1", "rt_rp", "off_rp"),
        "error1, with routing policy: rt_rp [realtime], off_rp [offline]");
  }

  @Test
  public void testQueryHashRegisteredInMdc() {
    String queryHash = "test_hash_abc123";
    LoggerConstants.QUERY_HASH_KEY.registerInMdc(queryHash);
    String mdcValue = MDC.get(LoggerConstants.QUERY_HASH_KEY.getKey());
    Assert.assertNotNull(mdcValue, "QueryHash should be registered in MDC");
    Assert.assertEquals(mdcValue, queryHash, "MDC should contain the correct queryHash");
  }

  @Test
  public void testQueryHashNotRegisteredWhenNull() {
    String mdcValue = MDC.get(LoggerConstants.QUERY_HASH_KEY.getKey());
    Assert.assertNull(mdcValue, "Null queryHash should not be registered in MDC");
  }

  @Test
  public void testQueryHashAddedToQueryOptions() {
    String query = "SELECT * FROM myTable WHERE col = 100";
    PinotQuery pinotQuery = CalciteSqlParser.compileToPinotQuery(query);
    String queryHash = "generated_hash_xyz";
    pinotQuery.putToQueryOptions(
        CommonConstants.Broker.Request.QueryOptionKey.QUERY_HASH,
        queryHash);
    Assert.assertTrue(pinotQuery.getQueryOptions().containsKey(
        CommonConstants.Broker.Request.QueryOptionKey.QUERY_HASH),
        "QueryHash should be added to queryOptions");
    Assert.assertEquals(
        pinotQuery.getQueryOptions().get(CommonConstants.Broker.Request.QueryOptionKey.QUERY_HASH),
        queryHash,
        "QueryOptions should contain the correct queryHash value");
  }

  private BaseSingleStageBrokerRequestHandler createHybridHandlerWithTimeBoundary(
      AtomicReference<TableRouteInfo> capturedRouteInfo) {
    String offlineTableName = "myTable_OFFLINE";
    String realtimeTableName = "myTable_REALTIME";

    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("myTable")
        .addDateTime("created_15min", DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS")
        .build();

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName("myTable")).thenReturn("myTable");
    when(tableCache.getSchema("myTable")).thenReturn(schema);
    when(tableCache.getColumnNameMap("myTable")).thenReturn(Map.of("created_15min", "created_15min"));

    TableConfig offlineTableCfg = mock(TableConfig.class);
    TableConfig realtimeTableCfg = mock(TableConfig.class);
    TenantConfig tenant = new TenantConfig("tier_BROKER", "tier_SERVER", null);
    when(offlineTableCfg.getTenantConfig()).thenReturn(tenant);
    when(realtimeTableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(offlineTableName)).thenReturn(offlineTableCfg);
    when(tableCache.getTableConfig(realtimeTableName)).thenReturn(realtimeTableCfg);

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(offlineTableName)).thenReturn(true);
    when(routingManager.routingExists(realtimeTableName)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10000L);
    TimeBoundaryInfo timeBoundaryInfo = new TimeBoundaryInfo("created_15min", "1772109900000");
    when(routingManager.getTimeBoundaryInfo(offlineTableName)).thenReturn(timeBoundaryInfo);

    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("segment01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);

    QueryQuotaManager queryQuotaManager = mock(QueryQuotaManager.class);
    when(queryQuotaManager.acquire(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(queryQuotaManager.acquireApplication(anyString())).thenReturn(true);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);

    return new BaseSingleStageBrokerRequestHandler(config, "testBrokerId", new BrokerRequestIdGenerator(),
        routingManager, new AllowAllAccessControlFactory(), queryQuotaManager, tableCache,
        ThreadAccountantUtils.getNoOpAccountant(), null, null) {
      @Override
      public void start() {
      }

      @Override
      public void shutDown() {
      }

      @Override
      protected BrokerResponseNative processBrokerRequest(long requestId, BrokerRequest originalBrokerRequest,
          BrokerRequest serverBrokerRequest, TableRouteInfo route, long timeoutMs, ServerStats serverStats,
          RequestContext requestContext) {
        capturedRouteInfo.set(route);
        return BrokerResponseNative.empty();
      }

      @Override
      protected BrokerResponseNative processMvSplitBrokerRequest(long requestId,
          BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute, TableRouteInfo mvRoute,
          long timeoutMs, ServerStats serverStats, RequestContext requestContext)
          throws Exception {
        throw new UnsupportedOperationException("Not implemented in test");
      }
    };
  }

  private static void assertRangeFilter(BrokerRequest brokerRequest, String column, String expectedRange,
      String label) {
    Assert.assertNotNull(brokerRequest, label + ": broker request should exist");
    Expression filter = brokerRequest.getPinotQuery().getFilterExpression();
    // Walk past any AND nodes wrapping non-time-column predicates to find the RANGE on our column
    Function filterFunc = filter.getFunctionCall();
    if (FilterKind.AND.name().equals(filterFunc.getOperator())) {
      Expression rangeExpr = null;
      for (Expression operand : filterFunc.getOperands()) {
        Function fn = operand.getFunctionCall();
        if (FilterKind.RANGE.name().equals(fn.getOperator())
            && column.equals(fn.getOperands().get(0).getIdentifier().getName())) {
          rangeExpr = operand;
          break;
        }
      }
      Assert.assertNotNull(rangeExpr, label + ": expected a RANGE filter on " + column + " within AND");
      filterFunc = rangeExpr.getFunctionCall();
    }
    Assert.assertEquals(filterFunc.getOperator(), FilterKind.RANGE.name(),
        label + ": filter should be a RANGE");
    List<Expression> operands = filterFunc.getOperands();
    Assert.assertEquals(operands.get(0).getIdentifier().getName(), column);
    Assert.assertEquals(operands.get(1).getLiteral().getStringValue(), expectedRange);
  }

  @Test
  public void testTimeBoundaryMergesWithBetween()
      throws Exception {
    AtomicReference<TableRouteInfo> capturedRouteInfo = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHybridHandlerWithTimeBoundary(capturedRouteInfo);

    handler.handleRequest("SELECT * FROM myTable "
        + "WHERE created_15min BETWEEN 1772106300000 AND 1772113500000 LIMIT 10");

    TableRouteInfo routeInfo = capturedRouteInfo.get();
    Assert.assertNotNull(routeInfo, "processBrokerRequest should have been called");
    assertRangeFilter(routeInfo.getOfflineBrokerRequest(), "created_15min",
        "[1772106300000" + Range.DELIMITER + "1772109900000]", "Offline BETWEEN");
    assertRangeFilter(routeInfo.getRealtimeBrokerRequest(), "created_15min",
        "(1772109900000" + Range.DELIMITER + "1772113500000]", "Realtime BETWEEN");
  }

  @Test
  public void testTimeBoundaryMergesWithExplicitRange()
      throws Exception {
    AtomicReference<TableRouteInfo> capturedRouteInfo = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHybridHandlerWithTimeBoundary(capturedRouteInfo);

    handler.handleRequest("SELECT * FROM myTable "
        + "WHERE created_15min > 1772106300000 AND created_15min < 1772113500000 LIMIT 10");

    TableRouteInfo routeInfo = capturedRouteInfo.get();
    Assert.assertNotNull(routeInfo, "processBrokerRequest should have been called");
    assertRangeFilter(routeInfo.getOfflineBrokerRequest(), "created_15min",
        "(1772106300000" + Range.DELIMITER + "1772109900000]", "Offline explicit range");
    assertRangeFilter(routeInfo.getRealtimeBrokerRequest(), "created_15min",
        "(1772109900000" + Range.DELIMITER + "1772113500000)", "Realtime explicit range");
  }

  @Test
  public void testTimeBoundaryMergesWithOneSidedRange()
      throws Exception {
    AtomicReference<TableRouteInfo> capturedRouteInfo = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHybridHandlerWithTimeBoundary(capturedRouteInfo);

    // Query has only a lower bound — time boundary supplies the complementary bound for each side
    handler.handleRequest("SELECT * FROM myTable "
        + "WHERE created_15min > 1772106300000 LIMIT 10");

    TableRouteInfo routeInfo = capturedRouteInfo.get();
    Assert.assertNotNull(routeInfo, "processBrokerRequest should have been called");
    // Offline: query's > 1772106300000 merged with time boundary's <= 1772109900000
    assertRangeFilter(routeInfo.getOfflineBrokerRequest(), "created_15min",
        "(1772106300000" + Range.DELIMITER + "1772109900000]", "Offline one-sided");
    // Realtime: query's > 1772106300000 merged with time boundary's > 1772109900000
    // Tighter bound wins: > 1772109900000 with no upper bound
    assertRangeFilter(routeInfo.getRealtimeBrokerRequest(), "created_15min",
        "(1772109900000" + Range.DELIMITER + "*)", "Realtime one-sided");
  }

  @Test
  public void testTimeBoundaryMergesWithMixedFilters()
      throws Exception {
    AtomicReference<TableRouteInfo> capturedRouteInfo = new AtomicReference<>();
    BaseSingleStageBrokerRequestHandler handler = createHybridHandlerWithTimeBoundary(capturedRouteInfo);

    handler.handleRequest("SELECT * FROM myTable "
        + "WHERE created_15min BETWEEN 1772106300000 AND 1772113500000 "
        + "AND created_15min > 1772109900000 LIMIT 10");

    TableRouteInfo routeInfo = capturedRouteInfo.get();
    Assert.assertNotNull(routeInfo, "processBrokerRequest should have been called");
    // Offline merges to (1772109900000, 1772109900000] — an empty range (exclusive lower = inclusive upper),
    // but the handler doesn't prune it as always-false; the server handles that at execution time.
    assertRangeFilter(routeInfo.getOfflineBrokerRequest(), "created_15min",
        "(1772109900000" + Range.DELIMITER + "1772109900000]", "Offline mixed");
    // Realtime merges to (1772109900000, 1772113500000].
    assertRangeFilter(routeInfo.getRealtimeBrokerRequest(), "created_15min",
        "(1772109900000" + Range.DELIMITER + "1772113500000]", "Realtime mixed");
  }

  /**
   * Bug: FULL_REWRITE overwrites tableName to the MV table name, so
   * _queryQuotaManager.acquire(tableName) charges quota against the MV
   * instead of the base table. A throttled base table is effectively
   * bypassed when its quota allows no traffic but the MV has no quota entry.
   *
   * <p>Before fix: acquire("baseTable_OFFLINE") is never called; the MV
   * table passes because the mock only denies the base table name.
   * <p>After fix: the base table name is used for quota accounting and the
   * request is correctly rate-limited.
   */
  @Test
  public void testMvFullRewriteQuotaAccountedAgainstBaseTable()
      throws Exception {
    String baseOfflineTable = "baseTable_OFFLINE";
    String mvOfflineTable = "mv_baseTable_OFFLINE";
    String baseRawTable = "baseTable";
    String mvRawTable = "mv_baseTable";

    // MV query: exact same columns but issued against the MV
    String userSql = "SELECT ts, SUM(revenue) FROM baseTable GROUP BY ts LIMIT 100 "
        + "OPTION(useMaterializedView='true')";
    PinotQuery mvQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM mv_baseTable_OFFLINE GROUP BY ts LIMIT 100");

    MvRewritePlan plan = new MvRewritePlan(
        mvOfflineTable, MatchType.EXACT, ExecutionMode.FULL_REWRITE, mvQuery, null, 1.0);
    MvRewriteResult mvResult = new MvRewriteResult(List.of(mvOfflineTable), plan);

    MvQueryRewriteEngine mvEngine = mock(MvQueryRewriteEngine.class);
    when(mvEngine.tryRewrite(any(PinotQuery.class), anyString())).thenReturn(mvResult);

    Schema baseSchema = new Schema.SchemaBuilder()
        .setSchemaName(baseRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName(mvRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName(baseRawTable)).thenReturn(baseRawTable);
    when(tableCache.getSchema(baseRawTable)).thenReturn(baseSchema);
    when(tableCache.getSchema(mvRawTable)).thenReturn(mvSchema);
    when(tableCache.getColumnNameMap(anyString())).thenReturn(Map.of("ts", "ts", "revenue", "revenue"));
    TableConfig tableCfg = mock(TableConfig.class);
    TenantConfig tenant = new TenantConfig("t_BROKER", "t_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(baseOfflineTable)).thenReturn(tableCfg);
    when(tableCache.getTableConfig(mvOfflineTable)).thenReturn(tableCfg);

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(baseOfflineTable)).thenReturn(true);
    when(routingManager.routingExists(mvOfflineTable)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("seg01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);

    // Only deny the base table; the MV has no quota entry (returns true).
    QueryQuotaManager quotaManager = mock(QueryQuotaManager.class);
    when(quotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(quotaManager.acquireApplication(anyString())).thenReturn(true);
    // Base table is over quota; MV is not throttled.
    when(quotaManager.acquire(baseOfflineTable)).thenReturn(false);
    when(quotaManager.acquire(mvOfflineTable)).thenReturn(true);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    PinotConfiguration config = new PinotConfiguration();
    BrokerQueryEventListenerFactory.init(config);

    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "broker1", new BrokerRequestIdGenerator(),
            routingManager, new AllowAllAccessControlFactory(), quotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null, mvEngine) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId,
              BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest,
              TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            // Should not reach here — quota must reject before routing
            Assert.fail("processBrokerRequest should not be called when base table is over quota");
            return null;
          }

          @Override
          protected BrokerResponseNative processMvSplitBrokerRequest(long requestId,
              BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute, TableRouteInfo mvRoute,
              long timeoutMs, ServerStats serverStats, RequestContext requestContext) {
            Assert.fail("processMvSplitBrokerRequest should not be called when base table is over quota");
            return null;
          }
        };

    BrokerResponseNative response = (BrokerResponseNative) handler.handleRequest(userSql);
    Assert.assertNotNull(response);
    // The request must be rejected with TOO_MANY_REQUESTS because the base table is over quota
    Assert.assertEquals(response.getExceptionsSize(), 1,
        "Expected quota rejection exception but got: " + response.getExceptions());
    Assert.assertEquals(response.getExceptions().get(0).getErrorCode(),
        org.apache.pinot.spi.exception.QueryErrorCode.TOO_MANY_REQUESTS.getId(),
        "Expected TOO_MANY_REQUESTS error code");
  }

  /**
   * Bug: FULL_REWRITE overwrites tableName to the MV table name, so
   * accessControl.getRowColFilters(requesterIdentity, tableName) fetches RLS
   * policy for the MV table instead of the base table.
   *
   * <p>Before fix: getRowColFilters is called with the MV table name
   * "mv_baseTable_OFFLINE".
   * <p>After fix: getRowColFilters is called with the original base table
   * name "baseTable_OFFLINE".
   */
  @Test
  public void testMvFullRewriteRlsLookupUsesBaseTable()
      throws Exception {
    String baseOfflineTable = "baseTable_OFFLINE";
    String mvOfflineTable = "mv_baseTable_OFFLINE";
    String baseRawTable = "baseTable";
    String mvRawTable = "mv_baseTable";

    String userSql = "SELECT ts, SUM(revenue) FROM baseTable GROUP BY ts LIMIT 100 "
        + "OPTION(useMaterializedView='true')";
    PinotQuery mvQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT ts, SUM(revenue) FROM mv_baseTable_OFFLINE GROUP BY ts LIMIT 100");

    MvRewritePlan plan = new MvRewritePlan(
        mvOfflineTable, MatchType.EXACT, ExecutionMode.FULL_REWRITE, mvQuery, null, 1.0);
    MvRewriteResult mvResult = new MvRewriteResult(List.of(mvOfflineTable), plan);

    MvQueryRewriteEngine mvEngine = mock(MvQueryRewriteEngine.class);
    when(mvEngine.tryRewrite(any(PinotQuery.class), anyString())).thenReturn(mvResult);

    Schema baseSchema = new Schema.SchemaBuilder()
        .setSchemaName(baseRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();
    Schema mvSchema = new Schema.SchemaBuilder()
        .setSchemaName(mvRawTable)
        .addSingleValueDimension("ts", DataType.STRING)
        .addMetric("revenue", DataType.DOUBLE)
        .build();

    TableCache tableCache = mock(TableCache.class);
    when(tableCache.getActualTableName(baseRawTable)).thenReturn(baseRawTable);
    when(tableCache.getSchema(baseRawTable)).thenReturn(baseSchema);
    when(tableCache.getSchema(mvRawTable)).thenReturn(mvSchema);
    when(tableCache.getColumnNameMap(anyString())).thenReturn(Map.of("ts", "ts", "revenue", "revenue"));
    TableConfig tableCfg = mock(TableConfig.class);
    TenantConfig tenant = new TenantConfig("t_BROKER", "t_SERVER", null);
    when(tableCfg.getTenantConfig()).thenReturn(tenant);
    when(tableCache.getTableConfig(baseOfflineTable)).thenReturn(tableCfg);
    when(tableCache.getTableConfig(mvOfflineTable)).thenReturn(tableCfg);

    BrokerRoutingManager routingManager = mock(BrokerRoutingManager.class);
    when(routingManager.routingExists(baseOfflineTable)).thenReturn(true);
    when(routingManager.routingExists(mvOfflineTable)).thenReturn(true);
    when(routingManager.getQueryTimeoutMs(anyString())).thenReturn(10000L);
    RoutingTable rt = mock(RoutingTable.class);
    when(rt.getServerInstanceToSegmentsMap()).thenReturn(
        Map.of(new ServerInstance(new InstanceConfig("server01_9000")),
            new SegmentsToQuery(List.of("seg01"), List.of())));
    when(routingManager.getRoutingTable(any(), Mockito.anyLong())).thenReturn(rt);

    QueryQuotaManager quotaManager = mock(QueryQuotaManager.class);
    when(quotaManager.acquire(anyString())).thenReturn(true);
    when(quotaManager.acquireDatabase(anyString())).thenReturn(true);
    when(quotaManager.acquireApplication(anyString())).thenReturn(true);

    // Use a concrete AccessControl that allows everything but records the table passed to getRowColFilters.
    // A Mockito mock of an interface cannot reliably stub default interface methods, so we use an
    // anonymous implementation to avoid NPEs from un-stubbed default-method paths.
    List<String> capturedRlsTables = new java.util.ArrayList<>();
    AccessControl accessControl = new AccessControl() {
      @Override
      public org.apache.pinot.spi.auth.AuthorizationResult authorize(
          org.apache.pinot.spi.auth.broker.RequesterIdentity identity, BrokerRequest request) {
        return TableAuthorizationResult.success();
      }

      @Override
      public TableAuthorizationResult authorize(
          org.apache.pinot.spi.auth.broker.RequesterIdentity identity, Set<String> tables) {
        return TableAuthorizationResult.success();
      }

      @Override
      public org.apache.pinot.spi.auth.TableRowColAccessResult getRowColFilters(
          org.apache.pinot.spi.auth.broker.RequesterIdentity identity, String tableWithType) {
        capturedRlsTables.add(tableWithType);
        return TableRowColAccessResultImpl.unrestricted();
      }
    };

    AccessControlFactory accessControlFactory = mock(AccessControlFactory.class);
    when(accessControlFactory.create()).thenReturn(accessControl);

    BrokerMetrics.register(mock(BrokerMetrics.class));
    // Enable row/column-level auth so the RLS path is exercised
    PinotConfiguration config = new PinotConfiguration(
        Map.of(Broker.CONFIG_OF_BROKER_ENABLE_ROW_COLUMN_LEVEL_AUTH, "true"));
    BrokerQueryEventListenerFactory.init(config);

    BaseSingleStageBrokerRequestHandler handler =
        new BaseSingleStageBrokerRequestHandler(config, "broker1", new BrokerRequestIdGenerator(),
            routingManager, accessControlFactory, quotaManager, tableCache,
            ThreadAccountantUtils.getNoOpAccountant(), null, mvEngine) {
          @Override
          public void start() {
          }

          @Override
          public void shutDown() {
          }

          @Override
          protected BrokerResponseNative processBrokerRequest(long requestId,
              BrokerRequest originalBrokerRequest, BrokerRequest serverBrokerRequest,
              TableRouteInfo route, long timeoutMs, ServerStats serverStats,
              RequestContext requestContext) {
            return BrokerResponseNative.empty();
          }

          @Override
          protected BrokerResponseNative processMvSplitBrokerRequest(long requestId,
              BrokerRequest originalBrokerRequest, TableRouteInfo baseRoute, TableRouteInfo mvRoute,
              long timeoutMs, ServerStats serverStats, RequestContext requestContext) {
            return BrokerResponseNative.empty();
          }
        };

    handler.handleRequest(userSql);

    // getRowColFilters must have been called with the base table name, not the MV table
    Assert.assertFalse(capturedRlsTables.isEmpty(),
        "getRowColFilters should have been called");
    String rlsTable = capturedRlsTables.get(0);
    Assert.assertNotEquals(rlsTable, mvOfflineTable,
        "RLS filter lookup must NOT use MV table name but got: " + rlsTable);
    // The RLS lookup must use the original base table identity, not the MV table.
    // The table name stored in preRewriteTableName is the raw name from compileSingleStageBrokerRequest
    // (before type resolution), so we assert on baseRawTable, not baseOfflineTable.
    Assert.assertEquals(rlsTable, baseRawTable,
        "RLS filter lookup must use base table '" + baseRawTable
            + "' not MV table, but got: " + rlsTable);
  }
}
