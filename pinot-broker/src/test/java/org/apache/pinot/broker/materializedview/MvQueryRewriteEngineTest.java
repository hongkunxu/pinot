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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.pinot.broker.materializedview.strategy.ExactSubsumptionStrategy;
import org.apache.pinot.broker.materializedview.strategy.MvMatchStrategy;
import org.apache.pinot.common.minion.MvDefinitionMetadata;
import org.apache.pinot.common.minion.MvFreshness;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class MvQueryRewriteEngineTest {

  private MvMetadataCache.MvCacheEntry createEntry(String mvTableName, String baseTable, String definedSql) {
    return createEntry(mvTableName, baseTable, definedSql, MvFreshness.FRESH);
  }

  private MvMetadataCache.MvCacheEntry createEntry(String mvTableName, String baseTable, String definedSql,
      MvFreshness freshness) {
    MvDefinitionMetadata definition = new MvDefinitionMetadata(
        mvTableName,
        Collections.singletonList(baseTable),
        definedSql,
        new HashMap<>(),
        null);
    PinotQuery compiledQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
    return new MvMetadataCache.MvCacheEntry(definition, compiledQuery, 0L, freshness);
  }

  @Test
  public void testNoRewriteWhenNoCandidates() {
    MvMetadataCache cache = mock(MvMetadataCache.class);
    when(cache.getMvEntriesForBaseTable("orders")).thenReturn(null);

    MvQueryRewriteEngine engine = new MvQueryRewriteEngine(cache, List.of(new ExactSubsumptionStrategy()));
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");

    MvRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNull(result);
  }

  @Test
  public void testNoRewriteWhenEmptyCandidates() {
    MvMetadataCache cache = mock(MvMetadataCache.class);
    when(cache.getMvEntriesForBaseTable("orders")).thenReturn(new ArrayList<>());

    MvQueryRewriteEngine engine = new MvQueryRewriteEngine(cache, List.of(new ExactSubsumptionStrategy()));
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");

    MvRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNull(result);
  }

  @Test
  public void testRewriteSelectsBestMatch() {
    String definedSql1 = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String definedSql2 =
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders WHERE region = 'US' GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";

    MvMetadataCache.MvCacheEntry entry1 = createEntry("mv_all_OFFLINE", "orders", definedSql1);
    MvMetadataCache.MvCacheEntry entry2 = createEntry("mv_us_OFFLINE", "orders", definedSql2);

    MvMetadataCache cache = mock(MvMetadataCache.class);
    when(cache.getMvEntriesForBaseTable("orders")).thenReturn(List.of(entry1, entry2));

    MvQueryRewriteEngine engine = new MvQueryRewriteEngine(cache, List.of(new ExactSubsumptionStrategy()));

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);
    MvRewriteResult result = engine.tryRewrite(userQuery, "orders");

    assertNotNull(result);
    assertTrue(result.isHit());
    assertEquals(result.getHitMvName(), "mv_all_OFFLINE");
    assertEquals(result.getPlan().getCost(), 0.0);
    assertEquals(result.getCandidateNames(), List.of("mv_all_OFFLINE", "mv_us_OFFLINE"));
  }

  @Test
  public void testRewriteNoMatchForDifferentQuery() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    MvMetadataCache cache = mock(MvMetadataCache.class);
    when(cache.getMvEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MvQueryRewriteEngine engine = new MvQueryRewriteEngine(cache, List.of(new ExactSubsumptionStrategy()));
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT state, MAX(revenue) FROM orders GROUP BY state");

    MvRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit());
    assertNull(result.getHitMvName());
    assertEquals(result.getCandidateNames(), List.of("mv_orders_OFFLINE"));
  }

  @Test
  public void testStrategyExceptionHandledGracefully() {
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders",
        "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city");

    MvMatchStrategy throwingStrategy = mock(MvMatchStrategy.class);
    when(throwingStrategy.match(any(), any())).thenThrow(new RuntimeException("test error"));

    MvMetadataCache cache = mock(MvMetadataCache.class);
    when(cache.getMvEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MvQueryRewriteEngine engine = new MvQueryRewriteEngine(cache, List.of(throwingStrategy));
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(
        "SELECT city, SUM(revenue) FROM orders GROUP BY city");

    MvRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit());
    assertEquals(result.getCandidateNames(), List.of("mv_orders_OFFLINE"));
  }

  @Test
  public void testMultipleStrategiesFirstMatchWins() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";
    MvMetadataCache.MvCacheEntry entry = createEntry("mv_orders_OFFLINE", "orders", definedSql);

    PinotQuery expectedRewritten = CalciteSqlParser.compileToPinotQuery(userSql);
    expectedRewritten.getDataSource().setTableName("mv_orders_OFFLINE");

    MvMatchStrategy noOpStrategy = mock(MvMatchStrategy.class);
    when(noOpStrategy.match(any(), any())).thenReturn(null);

    MvMetadataCache cache = mock(MvMetadataCache.class);
    when(cache.getMvEntriesForBaseTable("orders")).thenReturn(List.of(entry));

    MvQueryRewriteEngine engine = new MvQueryRewriteEngine(cache,
        List.of(noOpStrategy, new ExactSubsumptionStrategy()));

    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);
    MvRewriteResult result = engine.tryRewrite(userQuery, "orders");

    assertNotNull(result);
    assertTrue(result.isHit());
    assertEquals(result.getHitMvName(), "mv_orders_OFFLINE");
  }

  @Test
  public void testSkipStaleMv() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";

    MvMetadataCache.MvCacheEntry staleEntry =
        createEntry("mv_stale_OFFLINE", "orders", definedSql, MvFreshness.STALE);

    MvMetadataCache cache = mock(MvMetadataCache.class);
    when(cache.getMvEntriesForBaseTable("orders")).thenReturn(List.of(staleEntry));

    MvQueryRewriteEngine engine = new MvQueryRewriteEngine(cache, List.of(new ExactSubsumptionStrategy()));
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MvRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit());
    assertEquals(result.getCandidateNames(), List.of("mv_stale_OFFLINE"));
  }

  @Test
  public void testSkipDegradedMv() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";

    MvMetadataCache.MvCacheEntry degradedEntry = createEntry("mv_degraded_OFFLINE", "orders", definedSql,
        MvFreshness.DEGRADED);

    MvMetadataCache cache = mock(MvMetadataCache.class);
    when(cache.getMvEntriesForBaseTable("orders")).thenReturn(List.of(degradedEntry));

    MvQueryRewriteEngine engine = new MvQueryRewriteEngine(cache, List.of(new ExactSubsumptionStrategy()));
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MvRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertFalse(result.isHit());
    assertEquals(result.getCandidateNames(), List.of("mv_degraded_OFFLINE"));
  }

  @Test
  public void testSkipStaleButUseFreshMv() {
    String definedSql = "SELECT city, SUM(revenue) AS sum_revenue FROM orders GROUP BY city";
    String userSql = "SELECT city, SUM(revenue) FROM orders GROUP BY city";

    MvMetadataCache.MvCacheEntry staleEntry =
        createEntry("mv_stale_OFFLINE", "orders", definedSql, MvFreshness.STALE);
    MvMetadataCache.MvCacheEntry freshEntry = createEntry("mv_fresh_OFFLINE", "orders", definedSql);

    MvMetadataCache cache = mock(MvMetadataCache.class);
    when(cache.getMvEntriesForBaseTable("orders")).thenReturn(List.of(staleEntry, freshEntry));

    MvQueryRewriteEngine engine = new MvQueryRewriteEngine(cache, List.of(new ExactSubsumptionStrategy()));
    PinotQuery userQuery = CalciteSqlParser.compileToPinotQuery(userSql);

    MvRewriteResult result = engine.tryRewrite(userQuery, "orders");
    assertNotNull(result);
    assertTrue(result.isHit());
    assertEquals(result.getHitMvName(), "mv_fresh_OFFLINE");
    assertEquals(result.getCandidateNames(), List.of("mv_stale_OFFLINE", "mv_fresh_OFFLINE"));
  }
}
