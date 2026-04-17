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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class MaterializedViewTaskGeneratorTest {

  @Test
  public void testEnsureLimitAddsDefaultWhenMissing() {
    String sql = "SELECT col1, SUM(col2) FROM myTable GROUP BY col1";
    String result = MaterializedViewTaskGenerator.ensureLimit(sql, 1_000_000);
    assertEquals(result, "SELECT col1, SUM(col2) FROM myTable GROUP BY col1 LIMIT 1000000");
  }

  @Test
  public void testEnsureLimitPreservesExistingLimit() {
    String sql = "SELECT col1, SUM(col2) FROM myTable GROUP BY col1 LIMIT 500";
    String result = MaterializedViewTaskGenerator.ensureLimit(sql, 1_000_000);
    assertEquals(result, "SELECT col1, SUM(col2) FROM myTable GROUP BY col1 LIMIT 500");
  }

  @Test
  public void testEnsureLimitPreservesExistingLimitCaseInsensitive() {
    String sql = "SELECT col1 FROM myTable limit 200";
    String result = MaterializedViewTaskGenerator.ensureLimit(sql, 1_000_000);
    assertEquals(result, "SELECT col1 FROM myTable limit 200");
  }

  @Test
  public void testEnsureLimitStripsTrailingSemicolon() {
    String sql = "SELECT col1 FROM myTable;";
    String result = MaterializedViewTaskGenerator.ensureLimit(sql, 1_000_000);
    assertEquals(result, "SELECT col1 FROM myTable LIMIT 1000000");
  }

  @Test
  public void testEnsureLimitWithWhereClause() {
    String sql = "SELECT col1 FROM myTable WHERE col2 > 10 GROUP BY col1";
    String result = MaterializedViewTaskGenerator.ensureLimit(sql, 500_000);
    assertEquals(result, "SELECT col1 FROM myTable WHERE col2 > 10 GROUP BY col1 LIMIT 500000");
  }

  @Test
  public void testEnsureLimitWithOrderByAndExistingLimit() {
    String sql = "SELECT col1 FROM myTable ORDER BY col1 LIMIT 100";
    String result = MaterializedViewTaskGenerator.ensureLimit(sql, 1_000_000);
    assertEquals(result, "SELECT col1 FROM myTable ORDER BY col1 LIMIT 100");
  }

  @Test
  public void testAppendTimeRangeNoWhereClause() {
    String sql = "SELECT col1, SUM(col2) FROM myTable GROUP BY col1";
    String result = MaterializedViewTaskGenerator.appendTimeRange(sql, "ts", "100", "200");
    assertEquals(result, "SELECT col1, SUM(col2) FROM myTable WHERE ts >= 100 AND ts < 200 GROUP BY col1");
  }

  @Test
  public void testAppendTimeRangeWithExistingWhere() {
    String sql = "SELECT col1 FROM myTable WHERE col2 = 'foo' GROUP BY col1";
    String result = MaterializedViewTaskGenerator.appendTimeRange(sql, "ts", "100", "200");
    assertEquals(result, "SELECT col1 FROM myTable WHERE col2 = 'foo' AND ts >= 100 AND ts < 200 GROUP BY col1");
  }

  @Test
  public void testAppendTimeRangeWithLimit() {
    String sql = "SELECT col1 FROM myTable GROUP BY col1 LIMIT 50";
    String result = MaterializedViewTaskGenerator.appendTimeRange(sql, "ts", "100", "200");
    assertEquals(result, "SELECT col1 FROM myTable WHERE ts >= 100 AND ts < 200 GROUP BY col1 LIMIT 50");
  }

  @Test
  public void testAppendTimeRangeStripsTrailingSemicolon() {
    String sql = "SELECT col1 FROM myTable;";
    String result = MaterializedViewTaskGenerator.appendTimeRange(sql, "ts", "100", "200");
    assertEquals(result, "SELECT col1 FROM myTable WHERE ts >= 100 AND ts < 200");
  }
}
