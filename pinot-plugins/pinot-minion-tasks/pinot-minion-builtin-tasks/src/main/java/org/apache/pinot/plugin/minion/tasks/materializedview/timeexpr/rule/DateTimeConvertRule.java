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

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.FormatMatcher;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.InferredTimeSpec;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprInferrer;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeFunctionRule;
import org.apache.pinot.spi.data.DateTimeFieldSpec;


/**
 * Rule for {@code dateTimeConvert(col, inputFormat, outputFormat, granularity[, timeZone])}.
 *
 * <p>Inferred format = {@code outputFormat} (expected to match MV spec verbatim).
 * Inferred granularity = {@code granularity}.
 *
 * <p>We additionally assert that the declared {@code inputFormat} matches the base column's
 * {@link DateTimeFieldSpec#getFormat()}. A mismatch here is silently wrong at runtime (the
 * base values get reinterpreted under a foreign format), which is a correctness hazard
 * worth catching at create time.
 */
public final class DateTimeConvertRule implements TimeFunctionRule {

  public static final String CANONICAL_NAME = "datetimeconvert";

  @Override
  public String canonicalName() {
    return CANONICAL_NAME;
  }

  @Override
  public boolean acceptsOperandCount(int count) {
    return count == 4 || count == 5;
  }

  @Override
  public InferredTimeSpec infer(Function func, String baseTimeColumnName, DateTimeFieldSpec baseTimeFieldSpec) {
    List<Expression> operands = func.getOperands();
    String op = func.getOperator();

    TimeExprInferrer.requireIdentifierEqualsBaseTimeCol(operands.get(0), baseTimeColumnName, op);
    String inputFmt = TimeExprInferrer.requireStringLiteral(operands.get(1), "inputFormat", op);
    String outputFmt = TimeExprInferrer.requireStringLiteral(operands.get(2), "outputFormat", op);
    String granularity = TimeExprInferrer.requireStringLiteral(operands.get(3), "granularity", op);
    if (operands.size() == 5) {
      TimeExprInferrer.requireStringLiteral(operands.get(4), "timeZone", op);
    }

    String baseFmt = baseTimeFieldSpec.getFormat();
    Preconditions.checkState(inputFmt.equals(baseFmt),
        "dateTimeConvert inputFormat '%s' does not match base time column '%s' format '%s'. "
            + "Using the wrong input format silently reinterprets base values and yields wrong results.",
        inputFmt, baseTimeColumnName, baseFmt);

    String reason = "dateTimeConvert output format is '" + outputFmt
        + "' and declared granularity is '" + granularity + "'";
    return new InferredTimeSpec(FormatMatcher.exact(outputFmt), granularity, reason);
  }
}
