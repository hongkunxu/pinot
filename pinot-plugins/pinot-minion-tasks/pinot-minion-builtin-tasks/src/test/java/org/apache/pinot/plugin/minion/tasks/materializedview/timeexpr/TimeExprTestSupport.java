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
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * Shared helpers for {@code timeexpr} unit tests. Builds {@link Expression} / {@link Function}
 * payloads without going through Calcite, so individual rules can be exercised in isolation.
 */
public final class TimeExprTestSupport {
  public static final String BASE_COL = "ts";

  private TimeExprTestSupport() {
  }

  public static DateTimeFieldSpec millisBaseSpec() {
    return new DateTimeFieldSpec(BASE_COL, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS");
  }

  public static DateTimeFieldSpec daysBaseSpec() {
    return new DateTimeFieldSpec(BASE_COL, FieldSpec.DataType.LONG, "1:DAYS:EPOCH", "1:DAYS");
  }

  public static DateTimeFieldSpec sdfBaseSpec() {
    return new DateTimeFieldSpec(BASE_COL, FieldSpec.DataType.STRING,
        "1:DAYS:SIMPLE_DATE_FORMAT:yyyy-MM-dd", "1:DAYS");
  }

  public static Expression identifier(String name) {
    return RequestUtils.getIdentifierExpression(name);
  }

  public static Expression stringLit(String value) {
    return RequestUtils.getLiteralExpression(value);
  }

  public static Expression longLit(long value) {
    return RequestUtils.getLiteralExpression(value);
  }

  public static Function func(String name, Expression... operands) {
    return RequestUtils.getFunctionExpression(name, operands).getFunctionCall();
  }

  public static Expression funcExpr(String name, Expression... operands) {
    return RequestUtils.getFunctionExpression(name, operands);
  }
}
