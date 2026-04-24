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

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.FormatMatcher;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.InferredTimeSpec;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprInferrer;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeFunctionRule;
import org.apache.pinot.spi.data.DateTimeFieldSpec;


/**
 * Rule for {@code toDateTime(col, pattern[, timeZone])}.
 *
 * <p>Output is a {@code STRING} rendered via the supplied Java
 * {@link java.text.SimpleDateFormat}-style pattern. The MV
 * {@link DateTimeFieldSpec#getFormat()} must therefore be of the form
 * {@code "<size>:<unit>:SIMPLE_DATE_FORMAT:<pattern>"} with {@code <pattern>} equal to
 * the function's pattern argument.
 *
 * <p>Granularity is <b>not</b> inferred by this rule — {@code toDateTime} carries no bucket
 * semantic. The analyzer still requires the MV fieldSpec to declare a non-empty granularity
 * string so downstream components (e.g. task-generator window sizing) do not explode.
 *
 * <p>Base column must be unitary EPOCH; {@code SIMPLE_DATE_FORMAT} bases must route through
 * {@code dateTimeConvert} first.
 */
public final class ToDateTimeRule implements TimeFunctionRule {

  public static final String CANONICAL_NAME = "todatetime";

  @Override
  public String canonicalName() {
    return CANONICAL_NAME;
  }

  @Override
  public boolean acceptsOperandCount(int count) {
    return count == 2 || count == 3;
  }

  @Override
  public InferredTimeSpec infer(Function func, String baseTimeColumnName, DateTimeFieldSpec baseTimeFieldSpec) {
    List<Expression> operands = func.getOperands();
    String op = func.getOperator();

    TimeExprInferrer.requireIdentifierEqualsBaseTimeCol(operands.get(0), baseTimeColumnName, op);
    String pattern = TimeExprInferrer.requireStringLiteral(operands.get(1), "pattern", op);
    if (operands.size() == 3) {
      TimeExprInferrer.requireStringLiteral(operands.get(2), "timeZone", op);
    }

    TimeExprInferrer.requireUnitaryEpochBaseFormat(baseTimeFieldSpec, op);

    String reason = "toDateTime output is a SIMPLE_DATE_FORMAT string with pattern '" + pattern
        + "'; MV format must be '<size>:<unit>:SIMPLE_DATE_FORMAT:" + pattern
        + "'. Granularity is not inferred from toDateTime and is not strictly checked";
    return new InferredTimeSpec(FormatMatcher.sdfPattern(pattern), null, reason);
  }
}
