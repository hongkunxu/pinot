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
package org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.rule;

import org.apache.pinot.common.request.Function;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.InferredTimeSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.BASE_COL;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.func;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.identifier;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.longLit;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.millisBaseSpec;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.sdfBaseSpec;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.stringLit;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class ToDateTimeRuleTest {

  private final ToDateTimeRule _rule = new ToDateTimeRule();

  @Test
  public void canonicalNameAndOperandCounts() {
    assertEquals(_rule.canonicalName(), "todatetime");
    assertFalse(_rule.acceptsOperandCount(1));
    assertTrue(_rule.acceptsOperandCount(2));
    assertTrue(_rule.acceptsOperandCount(3));
    assertFalse(_rule.acceptsOperandCount(4));
  }

  @Test
  public void happyPathTwoArgs() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("toDateTime", identifier(BASE_COL), stringLit("yyyy-MM-dd"));

    InferredTimeSpec spec = _rule.infer(f, BASE_COL, base);
    assertTrue(spec.formatMatcher().matches("1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd"));
    assertTrue(spec.formatMatcher().matches("100:HOURS:SIMPLE_DATE_FORMAT:yyyy-MM-dd"));
    assertNull(spec.expectedGranularity(), "toDateTime must not infer a granularity");
  }

  @Test
  public void happyPathThreeArgsWithTimeZone() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("toDateTime", identifier(BASE_COL), stringLit("yyyy-MM-dd HH:mm:ss"), stringLit("UTC"));

    InferredTimeSpec spec = _rule.infer(f, BASE_COL, base);
    // Pattern itself contains colons; the matcher must preserve them via split-limit-4.
    assertTrue(spec.formatMatcher().matches("1:SECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-dd HH:mm:ss"));
  }

  @Test
  public void rejectsMvFormatWithMismatchedPattern() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("toDateTime", identifier(BASE_COL), stringLit("yyyy-MM-dd"));

    InferredTimeSpec spec = _rule.infer(f, BASE_COL, base);
    assertFalse(spec.formatMatcher().matches("1:DAYS:SIMPLE_DATE_FORMAT:yyyyMMdd"));
    assertFalse(spec.formatMatcher().matches("1:DAYS:EPOCH"));
    assertFalse(spec.formatMatcher().matches(null));
  }

  @Test
  public void rejectsSdfBase() {
    DateTimeFieldSpec base = sdfBaseSpec();
    Function f = func("toDateTime", identifier(BASE_COL), stringLit("yyyy-MM-dd"));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("EPOCH"), t.getMessage());
  }

  @Test
  public void rejectsNonIdentifierFirstArg() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("toDateTime", stringLit(BASE_COL), stringLit("yyyy-MM-dd"));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().toLowerCase().contains("base time column"), t.getMessage());
  }

  @Test
  public void rejectsNonStringPattern() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("toDateTime", identifier(BASE_COL), longLit(1L));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("pattern"), t.getMessage());
  }
}
