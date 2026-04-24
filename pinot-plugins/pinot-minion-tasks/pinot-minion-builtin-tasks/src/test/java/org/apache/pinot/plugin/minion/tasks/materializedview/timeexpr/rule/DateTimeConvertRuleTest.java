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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class DateTimeConvertRuleTest {

  private final DateTimeConvertRule _rule = new DateTimeConvertRule();

  @Test
  public void canonicalNameAndOperandCounts() {
    assertEquals(_rule.canonicalName(), "datetimeconvert");
    assertFalse(_rule.acceptsOperandCount(3));
    assertTrue(_rule.acceptsOperandCount(4));
    assertTrue(_rule.acceptsOperandCount(5));
    assertFalse(_rule.acceptsOperandCount(6));
  }

  @Test
  public void happyPathFourArgs() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("dateTimeConvert",
        identifier(BASE_COL),
        stringLit("1:MILLISECONDS:EPOCH"),
        stringLit("1:DAYS:EPOCH"),
        stringLit("1:DAYS"));

    InferredTimeSpec spec = _rule.infer(f, BASE_COL, base);

    assertTrue(spec.formatMatcher().matches("1:DAYS:EPOCH"));
    assertEquals(spec.expectedGranularity(), "1:DAYS");
  }

  @Test
  public void happyPathFiveArgsWithTimeZone() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("dateTimeConvert",
        identifier(BASE_COL),
        stringLit("1:MILLISECONDS:EPOCH"),
        stringLit("1:HOURS:EPOCH"),
        stringLit("1:HOURS"),
        stringLit("UTC"));

    InferredTimeSpec spec = _rule.infer(f, BASE_COL, base);
    assertTrue(spec.formatMatcher().matches("1:HOURS:EPOCH"));
    assertEquals(spec.expectedGranularity(), "1:HOURS");
  }

  @Test
  public void happyPathSdfOutput() {
    // Output may be SDF too — the matcher is exact-equality and the inferrer just propagates.
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("dateTimeConvert",
        identifier(BASE_COL),
        stringLit("1:MILLISECONDS:EPOCH"),
        stringLit("1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd"),
        stringLit("1:DAYS"));

    InferredTimeSpec spec = _rule.infer(f, BASE_COL, base);
    assertTrue(spec.formatMatcher().matches("1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd"));
  }

  @Test
  public void rejectsFirstArgNotIdentifier() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("dateTimeConvert",
        stringLit(BASE_COL),
        stringLit("1:MILLISECONDS:EPOCH"),
        stringLit("1:DAYS:EPOCH"),
        stringLit("1:DAYS"));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("first argument must be the base time column"), t.getMessage());
  }

  @Test
  public void rejectsFirstArgWrongIdentifier() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("dateTimeConvert",
        identifier("eventTime"),
        stringLit("1:MILLISECONDS:EPOCH"),
        stringLit("1:DAYS:EPOCH"),
        stringLit("1:DAYS"));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("eventTime"), t.getMessage());
    assertTrue(t.getMessage().contains(BASE_COL), t.getMessage());
  }

  @Test
  public void rejectsNonStringLiteralArgument() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("dateTimeConvert",
        identifier(BASE_COL),
        longLit(1L),
        stringLit("1:DAYS:EPOCH"),
        stringLit("1:DAYS"));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("inputFormat"), t.getMessage());
  }

  @Test
  public void rejectsInputFormatMismatch() {
    DateTimeFieldSpec base = millisBaseSpec();
    Function f = func("dateTimeConvert",
        identifier(BASE_COL),
        stringLit("1:SECONDS:EPOCH"),  // base is millis
        stringLit("1:DAYS:EPOCH"),
        stringLit("1:DAYS"));

    Throwable t = expectThrows(IllegalStateException.class, () -> _rule.infer(f, BASE_COL, base));
    assertTrue(t.getMessage().contains("inputFormat"), t.getMessage());
    assertTrue(t.getMessage().contains("silently"), t.getMessage());
  }

  @Test
  public void happyPathSdfBaseAcceptedWhenInputFormatDeclaresSdf() {
    // dateTimeConvert is the escape hatch for SDF bases — input format must declare SDF.
    DateTimeFieldSpec base = sdfBaseSpec();
    Function f = func("dateTimeConvert",
        identifier(BASE_COL),
        stringLit("1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd"),
        stringLit("1:DAYS:EPOCH"),
        stringLit("1:DAYS"));

    InferredTimeSpec spec = _rule.infer(f, BASE_COL, base);
    assertTrue(spec.formatMatcher().matches("1:DAYS:EPOCH"));
  }
}
