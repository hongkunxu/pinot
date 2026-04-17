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
import org.apache.pinot.broker.materializedview.strategy.AggregationSubsumptionStrategy;
import org.apache.pinot.common.minion.MvDefinitionMetadata;
import org.apache.pinot.common.minion.MvFreshness;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class AggregationSubsumptionStrategyTest {

  private AggregationSubsumptionStrategy _strategy;

  @BeforeClass
  public void setUp() {
    _strategy = new AggregationSubsumptionStrategy();
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
  //  Basic aggregation matching
  // -----------------------------------------------------------------------

  @Test
  public void testBasicAggregationMatch() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getMvTableNameWithType(), "mv_orders_OFFLINE");
    assertEquals(result.getCost(), 6.0);
    assertEquals(result.getMvQuery().getDataSource().getTableName(), "mv_orders_OFFLINE");
  }

  @Test
  public void testSelectSubset() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_rev, COUNT(revenue) AS cnt FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);
    assertEquals(result.getMvQuery().getSelectList().size(), 2);
  }

  // -----------------------------------------------------------------------
  //  GROUP BY order insensitivity
  // -----------------------------------------------------------------------

  @Test
  public void testGroupByOrderInsensitive() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, state, SUM(revenue) FROM orders GROUP BY state, city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);
  }

  // -----------------------------------------------------------------------
  //  Alias handling
  // -----------------------------------------------------------------------

  @Test
  public void testAliasInsensitiveMatching() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) AS total_revenue FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);
  }

  @Test
  public void testRewrittenSelectPreservesUserAlias() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) AS r_sum FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMvQuery();

    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);

    assertEquals(selectList.get(0).getIdentifier().getName(), "city");

    // SUM(revenue) AS r_sum → SUM(sum_rev) AS r_sum
    Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    Function innerAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertNotNull(innerAgg);
    assertEquals(innerAgg.getOperator(), "sum");
    assertEquals(innerAgg.getOperands().get(0).getIdentifier().getName(), "sum_rev");
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "r_sum");
  }

  @Test
  public void testRewrittenSelectNoAlias() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    List<Expression> selectList = result.getMvQuery().getSelectList();
    assertEquals(selectList.size(), 2);

    assertEquals(selectList.get(0).getIdentifier().getName(), "city");
    // SUM(revenue) → SUM(sum_rev) (no alias)
    Function reAgg = selectList.get(1).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "sum");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "sum_rev");
  }

  // -----------------------------------------------------------------------
  //  GROUP BY mismatch
  // -----------------------------------------------------------------------

  @Test
  public void testNoMatchUserHasMoreGroupByColumns() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, state, SUM(revenue) FROM orders GROUP BY city, state");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testFinerMvGranularityBasicSum() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "Finer MV granularity should match via re-aggregation");
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMvQuery();
    assertNotNull(rewritten.getGroupByList(), "GROUP BY should be retained");
    assertEquals(rewritten.getGroupByList().size(), 1);
    assertEquals(rewritten.getGroupByList().get(0).getIdentifier().getName(), "city");

    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);
    assertEquals(selectList.get(0).getIdentifier().getName(), "city");
    // SUM(revenue) → SUM(sum_rev)
    Function reAgg = selectList.get(1).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "sum");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "sum_rev");
  }

  @Test
  public void testNoMatchDifferentGroupByColumns() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT state, SUM(revenue) FROM orders GROUP BY state");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  HAVING remap
  // -----------------------------------------------------------------------

  @Test
  public void testHavingKeptAsHaving() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city HAVING SUM(revenue) > 1000");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMvQuery();

    assertNotNull(rewritten.getGroupByList());
    assertNotNull(rewritten.getHavingExpression());
    assertNull(rewritten.getFilterExpression());

    // HAVING SUM(revenue) > 1000 → HAVING SUM(sum_rev) > 1000
    assertTrue(rewritten.getHavingExpression().toString().contains("sum_rev"));
  }

  @Test
  public void testNoMatchHavingReferencesUnknownAgg() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city HAVING AVG(revenue) > 100");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "HAVING references AVG(revenue) which is not in MV projection");
  }

  // -----------------------------------------------------------------------
  //  ORDER BY remap
  // -----------------------------------------------------------------------

  @Test
  public void testOrderByRemappedToMvColumn() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city ORDER BY SUM(revenue) DESC");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMvQuery();

    List<Expression> orderByList = rewritten.getOrderByList();
    assertNotNull(orderByList);
    assertEquals(orderByList.size(), 1);
    assertTrue(orderByList.get(0).toString().contains("sum_rev"));
  }

  @Test
  public void testOrderByDimensionColumn() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city ORDER BY city ASC");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertNotNull(result.getMvQuery().getOrderByList());
  }

  @Test
  public void testNoMatchOrderByUnknownExpression() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city ORDER BY AVG(revenue)");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  Residual WHERE filter
  // -----------------------------------------------------------------------

  @Test
  public void testResidualWhereOnGroupByColumn() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_rev FROM orders WHERE region = 'US' GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' AND city = 'NYC' GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 7.0);
    assertNotNull(result.getMvQuery().getFilterExpression());
  }

  @Test
  public void testNoMatchResidualWhereOnNonGroupByColumn() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE status = 'active' GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "Residual filter on non-GROUP-BY column 'status' is invalid for aggregation MV");
  }

  // -----------------------------------------------------------------------
  //  Shape rejection
  // -----------------------------------------------------------------------

  @Test
  public void testNoMatchScanUserVsAggMv() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery("SELECT city FROM orders");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  @Test
  public void testNoMatchAggUserVsScanMv() {
    String definedSql = "SELECT city, revenue FROM orders";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  SELECT expression not in MV
  // -----------------------------------------------------------------------

  @Test
  public void testNoMatchSelectExprNotInMv() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, AVG(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
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

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result);
  }

  // -----------------------------------------------------------------------
  //  Rewrite structure verification
  // -----------------------------------------------------------------------

  @Test
  public void testRewrittenQueryRetainsGroupBy() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMvQuery();

    assertNotNull(rewritten.getGroupByList());
    assertEquals(rewritten.getGroupByList().size(), 1);
    assertEquals(rewritten.getGroupByList().get(0).getIdentifier().getName(), "city");
    assertNull(rewritten.getHavingExpression());
    assertNull(rewritten.getFilterExpression());
  }

  @Test
  public void testRewrittenQueryHavingAndResidualSeparate() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_rev FROM orders WHERE region = 'US' GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' AND city = 'NYC' "
            + "GROUP BY city HAVING SUM(revenue) > 1000");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMvQuery();

    assertNotNull(rewritten.getGroupByList());

    // HAVING kept as HAVING with re-aggregation reference
    assertNotNull(rewritten.getHavingExpression());
    assertTrue(rewritten.getHavingExpression().toString().contains("sum_rev"));

    // Residual WHERE on dimension column
    assertNotNull(rewritten.getFilterExpression());
    assertTrue(rewritten.getFilterExpression().toString().contains("city"));
  }

  @Test
  public void testNoFilterWhenFiltersEqualAndNoHaving() {
    String definedSql =
        "SELECT city, SUM(revenue) AS sum_rev FROM orders WHERE region = 'US' GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);
    assertNull(result.getMvQuery().getFilterExpression());
    assertNotNull(result.getMvQuery().getGroupByList());
  }

  // =======================================================================
  //  Equal granularity with function mismatch (equivalence-based rewrite)
  // =======================================================================

  @Test
  public void testEqualGranularitySketchMismatchUsesAggregationRewrite() {
    String definedSql =
        "SELECT city, DISTINCTCOUNTRAWHLL(user_id) AS raw_hll FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, DISTINCTCOUNTHLL(user_id) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "Equal granularity with sketch mismatch should match via aggregation rewrite");
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMvQuery();

    // GROUP BY must be retained (not removed as in scan rewrite)
    assertNotNull(rewritten.getGroupByList());
    assertEquals(rewritten.getGroupByList().size(), 1);
    assertEquals(rewritten.getGroupByList().get(0).getIdentifier().getName(), "city");

    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);
    assertEquals(selectList.get(0).getIdentifier().getName(), "city");

    // DISTINCTCOUNTHLL(user_id) → DISTINCTCOUNTHLL(raw_hll)
    Function reAgg = selectList.get(1).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "distinctcounthll");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "raw_hll");
  }

  /**
   * Simulates the broker's {@code handleHLLLog2mOverride} injecting a
   * {@code Literal(8)} operand into the user query's DISTINCTCOUNTHLL.
   * Verifies that:
   * <ol>
   *   <li>The match still succeeds (operandsMatch ignores literals).</li>
   *   <li>The rewritten query preserves the injected log2m parameter.</li>
   * </ol>
   */
  @Test
  public void testEqualGranularitySketchWithBrokerInjectedLog2m() {
    String definedSql =
        "SELECT city, DISTINCTCOUNTRAWHLL(user_id) AS raw_hll FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, DISTINCTCOUNTHLL(user_id) FROM orders GROUP BY city");

    // Simulate handleHLLLog2mOverride: append Literal(8) to DISTINCTCOUNTHLL operands
    for (Expression selectExpr : userQuery.getSelectList()) {
      Function func = selectExpr.getFunctionCall();
      if (func != null && "distinctcounthll".equalsIgnoreCase(func.getOperator())) {
        func.addToOperands(RequestUtils.getLiteralExpression(8));
      }
    }

    MvRewritePlan result = _strategy.match(userQuery, entry);
    assertNotNull(result, "Should match despite broker-injected log2m literal");
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMvQuery();
    assertNotNull(rewritten.getGroupByList());

    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 2);

    // DISTINCTCOUNTHLL(user_id, 8) → DISTINCTCOUNTHLL(raw_hll, 8)
    Function reAgg = selectList.get(1).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "distinctcounthll");
    assertEquals(reAgg.getOperandsSize(), 2, "Rewritten expression must retain trailing literal");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "raw_hll");
    assertTrue(reAgg.getOperands().get(1).getLiteral() != null,
        "Second operand must be the preserved log2m literal");
  }

  // =======================================================================
  //  Finer MV granularity tests (aggregation rewrite)
  // =======================================================================

  @Test
  public void testFinerMvCountToSum() {
    String definedSql =
        "SELECT city, state, COUNT(revenue) AS cnt FROM orders GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, COUNT(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "COUNT -> SUM re-aggregation should work");
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMvQuery();
    List<Expression> selectList = rewritten.getSelectList();
    // COUNT(revenue) → SUM(cnt)
    Function reAgg = selectList.get(1).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "sum");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "cnt");
  }

  @Test
  public void testFinerMvSketchMergeDistinctCountHll() {
    String definedSql =
        "SELECT city, state, DISTINCTCOUNTRAWHLL(user_id) AS raw_hll "
            + "FROM orders GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, DISTINCTCOUNTHLL(user_id) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result, "DISTINCTCOUNTHLL from DISTINCTCOUNTRAWHLL should match");
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMvQuery();
    List<Expression> selectList = rewritten.getSelectList();
    // DISTINCTCOUNTHLL(user_id) → DISTINCTCOUNTHLL(raw_hll)
    Function reAgg = selectList.get(1).getFunctionCall();
    assertNotNull(reAgg);
    assertEquals(reAgg.getOperator(), "distinctcounthll");
    assertEquals(reAgg.getOperands().get(0).getIdentifier().getName(), "raw_hll");
  }

  @Test
  public void testFinerMvMixedAggregationTypes() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev, COUNT(revenue) AS cnt, "
            + "MAX(revenue) AS max_rev FROM orders GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue), COUNT(revenue), MAX(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMvQuery();
    List<Expression> selectList = rewritten.getSelectList();
    assertEquals(selectList.size(), 4);

    // city -> city (dimension)
    assertEquals(selectList.get(0).getIdentifier().getName(), "city");
    // SUM(revenue) → SUM(sum_rev)
    assertEquals(selectList.get(1).getFunctionCall().getOperator(), "sum");
    assertEquals(selectList.get(1).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "sum_rev");
    // COUNT(revenue) → SUM(cnt)
    assertEquals(selectList.get(2).getFunctionCall().getOperator(), "sum");
    assertEquals(selectList.get(2).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "cnt");
    // MAX(revenue) → MAX(max_rev)
    assertEquals(selectList.get(3).getFunctionCall().getOperator(), "max");
    assertEquals(selectList.get(3).getFunctionCall().getOperands().get(0).getIdentifier().getName(), "max_rev");
  }

  @Test
  public void testFinerMvPreservesUserAlias() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) AS total FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    PinotQuery rewritten = result.getMvQuery();
    List<Expression> selectList = rewritten.getSelectList();

    // SUM(revenue) AS total → SUM(sum_rev) AS total
    Function aliasFunc = selectList.get(1).getFunctionCall();
    assertNotNull(aliasFunc);
    assertEquals(aliasFunc.getOperator(), "as");
    // Inner: SUM(sum_rev)
    Function innerAgg = aliasFunc.getOperands().get(0).getFunctionCall();
    assertEquals(innerAgg.getOperator(), "sum");
    assertEquals(innerAgg.getOperands().get(0).getIdentifier().getName(), "sum_rev");
    // Alias: total
    assertEquals(aliasFunc.getOperands().get(1).getIdentifier().getName(), "total");
  }

  @Test
  public void testFinerMvWithHavingKeptAsHaving() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city HAVING SUM(revenue) > 1000");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMvQuery();
    // GROUP BY retained
    assertNotNull(rewritten.getGroupByList());
    assertEquals(rewritten.getGroupByList().size(), 1);
    // HAVING kept as HAVING (not converted to WHERE)
    assertNotNull(rewritten.getHavingExpression());
    // HAVING should reference SUM(sum_rev) > 1000
    String havingStr = rewritten.getHavingExpression().toString();
    assertTrue(havingStr.contains("sum_rev"), "HAVING should reference re-aggregated MV column");
  }

  @Test
  public void testFinerMvWithResidualWhere() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders WHERE region = 'US' "
            + "GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders WHERE region = 'US' AND city = 'NYC' GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 7.0);

    PinotQuery rewritten = result.getMvQuery();
    assertNotNull(rewritten.getGroupByList());
    assertNotNull(rewritten.getFilterExpression());
    assertTrue(rewritten.getFilterExpression().toString().contains("city"));
  }

  @Test
  public void testFinerMvRejectUnsupportedFunction() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, AVG(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "AVG has no equivalence rule registered");
  }

  @Test
  public void testFinerMvRejectSketchFunctionMismatch() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, DISTINCTCOUNTHLL(revenue) FROM orders GROUP BY city");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNull(result, "MV stores SUM, not DISTINCTCOUNTRAWHLL — sketch merge not possible");
  }

  @Test
  public void testFinerMvWithOrderBy() {
    String definedSql =
        "SELECT city, state, SUM(revenue) AS sum_rev FROM orders GROUP BY city, state";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city ORDER BY SUM(revenue) DESC");
    MvRewritePlan result = _strategy.match(userQuery, entry);

    assertNotNull(result);
    assertEquals(result.getCost(), 6.0);

    PinotQuery rewritten = result.getMvQuery();
    assertNotNull(rewritten.getOrderByList());
    assertEquals(rewritten.getOrderByList().size(), 1);
    // ORDER BY should reference SUM(sum_rev), wrapped in ordering
    String orderByStr = rewritten.getOrderByList().get(0).toString();
    assertTrue(orderByStr.contains("sum_rev"));
  }
}
