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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.broker.materializedview.strategy.ExactSubsumptionStrategy;
import org.apache.pinot.common.minion.MvDefinitionMetadata;
import org.apache.pinot.common.minion.MvFreshness;
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
    MvDefinitionMetadata definition = new MvDefinitionMetadata(
        mvTableName,
        Collections.singletonList(baseTable),
        definedSql,
        new HashMap<>(),
        null);
    PinotQuery compiledQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
    return new MvMetadataCache.MvCacheEntry(definition, compiledQuery, 0L, MvFreshness.FRESH);
  }

  @Test
  public void testExactMatch() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getMvTableNameWithType(), "mv_orders_OFFLINE");
    assertEquals(result.getCost(), 0.0);
    assertEquals(result.getMvQuery().getDataSource().getTableName(), "mv_orders_OFFLINE");
  }

  @Test
  public void testNoMatchDifferentSelect() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, AVG(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchDifferentGroupBy() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city, state");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchWhenUserHasExtraFilter() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE status = 'active' GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "Exact strategy should reject any residual WHERE filter");
  }

  @Test
  public void testNoMatchWhenUserExtendsFilterWithAnd() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders WHERE region = 'US' GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' AND status = 'active' GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "Exact strategy should reject any residual WHERE filter");
  }

  @Test
  public void testNoMatchUserFilterSubsetOfMv() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders WHERE region = 'US' AND status = 'active' "
            + "GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchCompletelyDifferentFilter() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders WHERE region = 'US' GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'EU' GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNullCompiledQuery() {
    MvDefinitionMetadata definition = new MvDefinitionMetadata(
        "mv_broken_OFFLINE",
        Collections.singletonList("orders"),
        null,
        new HashMap<>(),
        null);
    MvMetadataCache.MvCacheEntry entry = new MvMetadataCache.MvCacheEntry(
        definition, null, 0L, MvFreshness.FRESH);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testExactMatchNoGroupBy() {
    String definedSql = "SELECT * FROM orders WHERE status = 'active'";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_active_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 0.0);
  }

  @Test
  public void testExactMatchIgnoresSelectOrder() {
    String mvSql = "SELECT a, b, c FROM orders";
    String querySql = "SELECT c, a, b FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 0.0);
  }

  @Test
  public void testExactMatchIgnoresAliasDifference() {
    String mvSql = "SELECT a, SUM(b) AS b_sum FROM orders GROUP BY a";
    String querySql = "SELECT a, SUM(b) AS total_b FROM orders GROUP BY a";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 0.0);
  }

  @Test
  public void testRewrittenSelectPreservesUserAlias() {
    String mvSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    String querySql = "SELECT city, SUM(revenue) AS r_sum FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(querySql);
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMvQuery();
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
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMvQuery();
    List<org.apache.pinot.common.request.Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);

    assertEquals(selectList.get(0).getIdentifier().getName(), "city");
    // SUM(revenue) without user alias -> simple identifier "sum_rev" (MV column name, no alias wrapper)
    assertEquals(selectList.get(1).getIdentifier().getName(), "sum_rev");
  }
}
