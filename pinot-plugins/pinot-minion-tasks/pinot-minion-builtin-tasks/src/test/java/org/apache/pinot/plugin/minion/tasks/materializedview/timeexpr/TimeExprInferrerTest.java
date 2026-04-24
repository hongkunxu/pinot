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
package org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr;

import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.BASE_COL;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.funcExpr;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.identifier;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.longLit;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.millisBaseSpec;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.stringLit;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class TimeExprInferrerTest {

  @Test
  public void identityIdentifierIsAccepted() {
    DateTimeFieldSpec base = millisBaseSpec();
    InferredTimeSpec spec = TimeExprInferrer.infer(identifier(BASE_COL), BASE_COL, base);

    assertTrue(spec.formatMatcher().matches("1:MILLISECONDS:EPOCH"));
    assertEquals(spec.expectedGranularity(), "1:MILLISECONDS");
  }

  @Test
  public void identityIdentifierWithWrongNameIsRejected() {
    DateTimeFieldSpec base = millisBaseSpec();
    Throwable t = expectThrows(IllegalStateException.class,
        () -> TimeExprInferrer.infer(identifier("eventTime"), BASE_COL, base));
    assertTrue(t.getMessage().contains(BASE_COL), t.getMessage());
  }

  @Test
  public void literalExpressionIsRejected() {
    DateTimeFieldSpec base = millisBaseSpec();
    Throwable t = expectThrows(IllegalStateException.class,
        () -> TimeExprInferrer.infer(stringLit("foo"), BASE_COL, base));
    assertTrue(t.getMessage().contains("identity or a whitelisted") || t.getMessage().contains("expression type"),
        t.getMessage());
  }

  @Test
  public void arithmeticFunctionIsRejected() {
    DateTimeFieldSpec base = millisBaseSpec();
    Expression expr = funcExpr("divide", identifier(BASE_COL), longLit(1000L));

    Throwable t = expectThrows(IllegalStateException.class,
        () -> TimeExprInferrer.infer(expr, BASE_COL, base));
    assertTrue(t.getMessage().toLowerCase().contains("unsupported function"), t.getMessage());
    assertTrue(t.getMessage().contains("dateTimeConvert".toLowerCase()) || t.getMessage().contains("datetimeconvert"),
        t.getMessage());
  }

  @Test
  public void unknownFunctionIsRejected() {
    DateTimeFieldSpec base = millisBaseSpec();
    Expression expr = funcExpr("fromEpochDays", identifier(BASE_COL));

    Throwable t = expectThrows(IllegalStateException.class,
        () -> TimeExprInferrer.infer(expr, BASE_COL, base));
    assertTrue(t.getMessage().toLowerCase().contains("unsupported function"), t.getMessage());
  }

  @Test
  public void nestedFunctionIsRejected() {
    // dateTimeConvert(date_trunc('DAY', ts), '1:DAYS:EPOCH', '1:DAYS:EPOCH', '1:DAYS')
    DateTimeFieldSpec base = millisBaseSpec();
    Expression nested = funcExpr("date_trunc", stringLit("DAY"), identifier(BASE_COL));
    Expression expr = funcExpr("dateTimeConvert",
        nested,
        stringLit("1:DAYS:EPOCH"),
        stringLit("1:DAYS:EPOCH"),
        stringLit("1:DAYS"));

    Throwable t = expectThrows(IllegalStateException.class,
        () -> TimeExprInferrer.infer(expr, BASE_COL, base));
    // The first-arg-must-be-identifier check fires inside DateTimeConvertRule.
    assertTrue(t.getMessage().contains("first argument must be the base time column"), t.getMessage());
  }

  @Test
  public void wrongOperandCountIsRejectedWithFunctionName() {
    DateTimeFieldSpec base = millisBaseSpec();
    // dateTimeConvert with only 2 args
    Expression expr = funcExpr("dateTimeConvert", identifier(BASE_COL), stringLit("1:MILLISECONDS:EPOCH"));

    Throwable t = expectThrows(IllegalStateException.class,
        () -> TimeExprInferrer.infer(expr, BASE_COL, base));
    assertTrue(t.getMessage().contains("dateTimeConvert"), t.getMessage());
    assertTrue(t.getMessage().contains("not supported") || t.getMessage().contains("arguments"), t.getMessage());
  }

  @Test
  public void canonicalizationLetsUnderscoreAndCamelCaseMapToSameRule() {
    DateTimeFieldSpec base = millisBaseSpec();
    // dateTimeConvert and date_time_convert both canonicalize to "datetimeconvert".
    Expression a = funcExpr("dateTimeConvert", identifier(BASE_COL), stringLit("1:MILLISECONDS:EPOCH"),
        stringLit("1:DAYS:EPOCH"), stringLit("1:DAYS"));
    Expression b = funcExpr("date_time_convert", identifier(BASE_COL), stringLit("1:MILLISECONDS:EPOCH"),
        stringLit("1:DAYS:EPOCH"), stringLit("1:DAYS"));

    InferredTimeSpec specA = TimeExprInferrer.infer(a, BASE_COL, base);
    InferredTimeSpec specB = TimeExprInferrer.infer(b, BASE_COL, base);
    assertTrue(specA.formatMatcher().matches("1:DAYS:EPOCH"));
    assertTrue(specB.formatMatcher().matches("1:DAYS:EPOCH"));
  }

  @Test
  public void registryListsAllThreeWhitelistedFunctions() {
    assertEquals(TimeFunctionRegistry.supportedCanonicalNames().size(), 3);
    assertTrue(TimeFunctionRegistry.supportedCanonicalNames().contains("datetimeconvert"));
    assertTrue(TimeFunctionRegistry.supportedCanonicalNames().contains("datetrunc"));
    assertTrue(TimeFunctionRegistry.supportedCanonicalNames().contains("todatetime"));
  }

  @Test
  public void identityRuleOnlyAcceptsBaseColumn() {
    // Use the dispatched path through TimeExprInferrer so we know the registry doesn't intercept.
    DateTimeFieldSpec base = millisBaseSpec();
    // RequestUtils.getIdentifierExpression preserves casing — but Pinot identifiers are case-sensitive at this layer.
    Expression mismatched = RequestUtils.getIdentifierExpression("TS");
    Throwable t = expectThrows(IllegalStateException.class,
        () -> TimeExprInferrer.infer(mismatched, BASE_COL, base));
    assertTrue(t.getMessage().contains(BASE_COL), t.getMessage());
  }
}
