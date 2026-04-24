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
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.daysBaseSpec;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.func;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.identifier;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.longLit;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.millisBaseSpec;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.sdfBaseSpec;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.stringLit;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class DateTruncRuleTest {

  private final DateTruncRule _rule = new DateTruncRule();

  @Test
  public void canonicalNameAndOperandCounts() {
    assertEquals(_rule.canonicalName(), "datetrunc");
    assertFalse(_rule.acceptsOperandCount(1));
    assertTrue(_rule.acceptsOperandCount(2));
    assertTrue(_rule.acceptsOperandCount(5));
    assertFalse(_rule.acceptsOperandCount(6));
  }

  @Test
  public void happyPathTwoArgsMillisBase() {
    // date_trunc('DAY', ts) with millis base => format reflects storage (MILLISECONDS), not granularity.
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("date_trunc", stringLit("DAY"), identifier(BASE_COL));

    InferredTimeSpec spec = _rule.infer(f, BASE_COL, base);
    assertTrue(spec.formatMatcher().matches("1:MILLISECONDS:EPOCH"));
    assertEquals(spec.expectedGranularity(), "1:DAYS");
    assertTrue(spec.reason().contains("storage unit"), spec.reason());
  }

  @Test
  public void happyPathFiveArgsExplicitOutputUnit() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("date_trunc",
        stringLit("HOUR"),
        identifier(BASE_COL),
        stringLit("MILLISECONDS"),
        stringLit("UTC"),
        stringLit("HOURS"));

    InferredTimeSpec spec = _rule.infer(f, BASE_COL, base);
    assertTrue(spec.formatMatcher().matches("1:HOURS:EPOCH"));
    assertEquals(spec.expectedGranularity(), "1:HOURS");
  }

  @Test
  public void happyPathExplicitInputUnitMatchingNonMillisBase() {
    DateTimeFieldSpec base = daysBaseSpec();
    Function f = func("date_trunc",
        stringLit("DAY"),
        identifier(BASE_COL),
        stringLit("DAYS"));

    InferredTimeSpec spec = _rule.infer(f, BASE_COL, base);
    assertTrue(spec.formatMatcher().matches("1:DAYS:EPOCH"));
    assertEquals(spec.expectedGranularity(), "1:DAYS");
  }

  @Test
  public void rejectsUnsupportedTruncationUnit() {
    // WEEK / MONTH / QUARTER / YEAR have no java.util.concurrent.TimeUnit equivalent and
    // therefore cannot be encoded into the MV DateTimeFieldSpec granularity in v1.
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("date_trunc", stringLit("WEEK"), identifier(BASE_COL));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("WEEK"), t.getMessage());
    assertTrue(t.getMessage().contains("dateTimeConvert"), t.getMessage());
  }

  @Test
  public void rejectsImplicitMillisDefaultAgainstNonMillisBase() {
    // Pinot defaults inputTimeUnit to MILLISECONDS when omitted; if base is DAYS this silently
    // reinterprets timestamps. We must reject here.
    DateTimeFieldSpec base = daysBaseSpec();
    Function f = func("date_trunc", stringLit("DAY"), identifier(BASE_COL));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("MILLISECONDS"), t.getMessage());
    assertTrue(t.getMessage().contains("DAYS"), t.getMessage());
  }

  @Test
  public void rejectsExplicitInputUnitMismatch() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("date_trunc",
        stringLit("DAY"),
        identifier(BASE_COL),
        stringLit("SECONDS"));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("inputTimeUnit"), t.getMessage());
  }

  @Test
  public void rejectsSdfBase() {
    DateTimeFieldSpec base = sdfBaseSpec();
    Function f = func("date_trunc", stringLit("DAY"), identifier(BASE_COL));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("EPOCH"), t.getMessage());
    assertTrue(t.getMessage().contains("dateTimeConvert"), t.getMessage());
  }

  @Test
  public void rejectsNonStringUnitArg() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("date_trunc", longLit(1L), identifier(BASE_COL));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("'unit'"), t.getMessage());
  }

  @Test
  public void rejectsFirstColArgNotIdentifier() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("date_trunc", stringLit("DAY"), longLit(1L));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().toLowerCase().contains("base time column"), t.getMessage());
  }
}
