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
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Unit tests for {@link MaterializedViewTaskExecutor#verifyResultNotTruncated}.
 *
 * <p>This is the single-most-important correctness gate in the MV executor: if the query result
 * saturates the declared LIMIT, we MUST fail the task so {@code coverageUpperMs} is not advanced
 * against incomplete data.
 */
public class MaterializedViewTaskExecutorTest {

  private static final String TABLE = "mv_orders";
  private static final long WINDOW_START = 1_700_000_000_000L;
  private static final long WINDOW_END = 1_700_086_400_000L;

  @Test
  public void testUnderLimitPasses() {
    Map<String, String> configs = configsWithLimit(1_000);
    MaterializedViewTaskExecutor.verifyResultNotTruncated(
        configs, TABLE, WINDOW_START, WINDOW_END, 999);
  }

  @Test
  public void testAtLimitFails() {
    Map<String, String> configs = configsWithLimit(1_000);
    try {
      MaterializedViewTaskExecutor.verifyResultNotTruncated(
          configs, TABLE, WINDOW_START, WINDOW_END, 1_000);
      fail("Expected completeness gate to fail when rows == LIMIT");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("saturated LIMIT"),
          "Unexpected message: " + e.getMessage());
      assertTrue(e.getMessage().contains(TABLE));
    }
  }

  @Test
  public void testOverLimitFails() {
    // Defensive: the broker should cap at LIMIT, but an ill-behaved executor could return more.
    Map<String, String> configs = configsWithLimit(1_000);
    try {
      MaterializedViewTaskExecutor.verifyResultNotTruncated(
          configs, TABLE, WINDOW_START, WINDOW_END, 1_500);
      fail("Expected completeness gate to fail when rows > LIMIT");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("saturated LIMIT"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testMissingLimitKeyIsBackwardsCompatible() {
    // Pre-upgrade tasks may not carry EFFECTIVE_LIMIT_KEY; we WARN but do not fail to avoid
    // breaking in-flight work during a rolling upgrade.  Steady-state tasks always carry it.
    Map<String, String> configs = new HashMap<>();
    MaterializedViewTaskExecutor.verifyResultNotTruncated(
        configs, TABLE, WINDOW_START, WINDOW_END, 42);
  }

  @Test
  public void testInvalidLimitKeyThrows() {
    Map<String, String> configs = new HashMap<>();
    configs.put(MaterializedViewTask.EFFECTIVE_LIMIT_KEY, "not-a-number");
    try {
      MaterializedViewTaskExecutor.verifyResultNotTruncated(
          configs, TABLE, WINDOW_START, WINDOW_END, 42);
      fail("Expected IllegalStateException for malformed effectiveLimit");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Invalid"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testNonPositiveLimitThrows() {
    Map<String, String> configs = configsWithLimit(0);
    try {
      MaterializedViewTaskExecutor.verifyResultNotTruncated(
          configs, TABLE, WINDOW_START, WINDOW_END, 42);
      fail("Expected IllegalStateException for non-positive effectiveLimit");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("must be positive"),
          "Unexpected message: " + e.getMessage());
    }
  }

  @Test
  public void testZeroRowsPasses() {
    // Empty windows are legitimate and must not be flagged as truncated.
    Map<String, String> configs = configsWithLimit(1_000);
    MaterializedViewTaskExecutor.verifyResultNotTruncated(
        configs, TABLE, WINDOW_START, WINDOW_END, 0);
  }

  private static Map<String, String> configsWithLimit(int limit) {
    Map<String, String> configs = new HashMap<>();
    configs.put(MaterializedViewTask.EFFECTIVE_LIMIT_KEY, String.valueOf(limit));
    return configs;
  }
}
