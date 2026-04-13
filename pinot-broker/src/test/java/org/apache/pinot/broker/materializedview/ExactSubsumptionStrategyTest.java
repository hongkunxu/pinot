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
package org.apache.pinot.broker.materializedview;

import java.util.HashMap;
import java.util.List;
import org.apache.pinot.broker.materializedview.strategy.ExactSubsumptionStrategy;
import org.apache.pinot.common.minion.MaterializedViewMetadata;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ExactSubsumptionStrategyTest {

  private ExactSubsumptionStrategy _strategy;

  @BeforeClass
  public void setUp() {
    _strategy = new ExactSubsumptionStrategy();
  }

  private MvMetadataCache.MvCacheEntry createEntry(String mvTableName, String baseTable, String definedSql) {
    MaterializedViewMetadata metadata = new MaterializedViewMetadata(
        mvTableName,
        List.of(baseTable),
        baseTable,
        definedSql,
        new HashMap<>(),
        new HashMap<>(),
        new HashMap<>()
    );
    PinotQuery compiledQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
    return new MvMetadataCache.MvCacheEntry(metadata, compiledQuery);
  }

  @Test
  public void testExactMatch() {
    String definedSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getMvTableName(), "mv_orders_OFFLINE");
    assertEquals(result.getCost(), 0.0);
    assertEquals(result.getRewrittenQuery().getDataSource().getTableName(), "mv_orders_OFFLINE");
  }

  @Test
  public void testNoMatchDifferentSelect() {
    String definedSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, AVG(revenue) FROM orders GROUP BY city");
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchDifferentGroupBy() {
    String definedSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city, state");
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchWhenUserHasExtraFilter() {
    String definedSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE status = 'active' GROUP BY city");
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNull(result, "Exact strategy should reject any residual WHERE filter");
  }

  @Test
  public void testNoMatchWhenUserExtendsFilterWithAnd() {
    String definedSql = "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' AND status = 'active' GROUP BY city");
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNull(result, "Exact strategy should reject any residual WHERE filter");
  }

  @Test
  public void testNoMatchUserFilterSubsetOfMv() {
    String definedSql =
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' AND status = 'active' GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' GROUP BY city");
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchCompletelyDifferentFilter() {
    String definedSql = "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'EU' GROUP BY city");
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNullCompiledQuery() {
    MaterializedViewMetadata metadata = new MaterializedViewMetadata(
        "mv_broken_OFFLINE",
        List.of("orders"),
        "orders",
        null,
        new HashMap<>(),
        new HashMap<>(),
        new HashMap<>()
    );
    MvMetadataCache.MvCacheEntry entry = new MvMetadataCache.MvCacheEntry(metadata, null);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testExactMatchNoGroupBy() {
    String definedSql = "SELECT * FROM orders WHERE status = 'active'";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_active_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 0.0);
  }

  @Test
  public void testExactMatchIgnoresSelectOrder() {
    String mvSql = "SELECT a, b, c FROM orders";
    String querySql = "SELECT c, a, b FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 0.0);
  }

  @Test
  public void testExactMatchIgnoresAliasDifference() {
    String mvSql = "SELECT a, SUM(b) AS b_sum FROM orders GROUP BY a";
    String querySql = "SELECT a, SUM(b) AS total_b FROM orders GROUP BY a";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 0.0);
  }

  @Test
  public void testRewrittenSelectPreservesUserAlias() {
    String mvSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    String querySql = "SELECT city, SUM(revenue) AS r_sum FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getRewrittenQuery();
    assertEquals(rewritten.getDataSource().getTableName(), "mv_orders_OFFLINE");
    assertNull(rewritten.getFilterExpression());

    List<org.apache.pinot.common.request.Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);

    // "city" column (no alias in user query) -> simple identifier "city"
    assertEquals(selectList.get(0).getIdentifier().getName(), "city");

    // "SUM(revenue) AS r_sum" -> rewritten to "sum_rev AS r_sum"
    org.apache.pinot.common.request.Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    assertEquals(aliasFunc.getOperands().get(0).getIdentifier().getName(), "sum_rev");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "r_sum");
  }

  @Test
  public void testRewrittenSelectNoAlias() {
    String mvSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    String querySql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MvMatchResult result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getRewrittenQuery();
    List<org.apache.pinot.common.request.Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);

    assertEquals(selectList.get(0).getIdentifier().getName(), "city");
    // SUM(revenue) without user alias -> simple identifier "sum_rev" (MV column name, no alias wrapper)
    assertEquals(selectList.get(1).getIdentifier().getName(), "sum_rev");
  }
}
