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
import org.apache.pinot.broker.materializedview.strategy.ScanSubsumptionStrategy;
import org.apache.pinot.common.minion.MvDefinitionMetadata;
import org.apache.pinot.common.minion.MvFreshness;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class ScanSubsumptionStrategyTest {

  private ScanSubsumptionStrategy _strategy;

  @BeforeClass
  public void setUp() {
    _strategy = new ScanSubsumptionStrategy();
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

  // -----------------------------------------------------------------------
  //  Projection subset matching
  // -----------------------------------------------------------------------

  @Test
  public void testProjectionSubset() {
    String mvSql = "SELECT a, b, c FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT a FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 2.0);
    assertEquals(result.getMvQuery().getDataSource().getTableName(), "mv_orders_OFFLINE");
    assertEquals(result.getMvQuery().getSelectList().size(), 1);
  }

  @Test
  public void testProjectionSubsetMultipleColumns() {
    String mvSql = "SELECT a, b, c FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT a, c FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 2.0);
  }

  @Test
  public void testProjectionSubsetDifferentOrder() {
    String mvSql = "SELECT a, b, c FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT c, a FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 2.0);
  }

  @Test
  public void testNoMatchColumnNotInMv() {
    String mvSql = "SELECT a, b FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT a, d FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  WHERE / residual filter
  // -----------------------------------------------------------------------

  @Test
  public void testProjectionSubsetWithResidualFilter() {
    String mvSql = "SELECT a, b, c FROM orders WHERE region = 'US'";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT a FROM orders WHERE region = 'US' AND b = 'active'");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 3.0);
    assertNotNull(result.getMvQuery().getFilterExpression());
  }

  @Test
  public void testNoMatchResidualFilterReferencesColumnNotInMv() {
    String mvSql = "SELECT a, b FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT a FROM orders WHERE c = 1");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testProjectionSubsetMvNoFilterUserNoFilter() {
    String mvSql = "SELECT a, b, c FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT a, b FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 2.0);
    assertNull(result.getMvQuery().getFilterExpression());
  }

  // -----------------------------------------------------------------------
  //  Shape rejection
  // -----------------------------------------------------------------------

  @Test
  public void testNoMatchWhenQueryHasGroupBy() {
    String mvSql = "SELECT city, revenue FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchWhenMvHasGroupBy() {
    String mvSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT city FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  ORDER BY
  // -----------------------------------------------------------------------

  @Test
  public void testNoMatchOrderByColumnNotInMv() {
    String mvSql = "SELECT a, b FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT a FROM orders ORDER BY c");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testMatchOrderByColumnInMv() {
    String mvSql = "SELECT a, b, c FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT a FROM orders ORDER BY b");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 2.0);
  }

  // -----------------------------------------------------------------------
  //  Null compiled query
  // -----------------------------------------------------------------------

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

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT a FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  SELECT rewrite verification
  // -----------------------------------------------------------------------

  @Test
  public void testRewrittenSelectUsesOriginalColumnNames() {
    String mvSql = "SELECT a, b, c FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT b, a FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    List<org.apache.pinot.common.request.Expression> rewrittenSelect =
        result.getMvQuery().getSelectList();
    assertEquals(rewrittenSelect.size(), 2);
    assertEquals(rewrittenSelect.get(0).getIdentifier().getName(), "b");
    assertEquals(rewrittenSelect.get(1).getIdentifier().getName(), "a");
  }

  @Test
  public void testRewrittenSelectPreservesUserAlias() {
    String mvSql = "SELECT a, b AS col_b FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT b AS my_b FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    List<org.apache.pinot.common.request.Expression> selectList =
        result.getMvQuery().getSelectList();
    assertEquals(selectList.size(), 1);

    // "b AS my_b" -> rewritten to "col_b AS my_b"
    org.apache.pinot.common.request.Function aliasFunc = selectList.get(0).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    assertEquals(aliasFunc.getOperands().get(0).getIdentifier().getName(), "col_b");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "my_b");
  }

  @Test
  public void testRewrittenSelectNoAliasWhenUserHasNoAlias() {
    String mvSql = "SELECT a, b AS col_b FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", mvSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT b FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    List<org.apache.pinot.common.request.Expression> selectList =
        result.getMvQuery().getSelectList();
    assertEquals(selectList.size(), 1);

    // "b" (no user alias) -> simple identifier "col_b" (MV column name, no alias wrapper)
    assertEquals(selectList.get(0).getIdentifier().getName(), "col_b");
  }
}
