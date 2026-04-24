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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.FormatMatcher;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.InferredTimeSpec;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprInferrer;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeFunctionRule;
import org.apache.pinot.spi.data.DateTimeFieldSpec;


/**
 * Rule for {@code date_trunc(unit, col[, inputTimeUnit[, timeZone[, outputTimeUnit]]])}.
 *
 * <p>Argument positions (mirroring
 * {@link org.apache.pinot.common.function.scalar.DateTimeFunctions#dateTrunc}):
 * <pre>
 *   0: unit               string literal, e.g. 'DAY', 'HOUR'
 *   1: col                base time column identifier
 *   2: inputTimeUnit      (optional) defaults to MILLISECONDS (Pinot function default)
 *   3: timeZone           (optional) defaults to UTC
 *   4: outputTimeUnit     (optional) defaults to inputTimeUnit
 * </pre>
 *
 * <p>Output characteristics:
 * <ul>
 *   <li>Inferred format: {@code "1:<outputTimeUnit>:EPOCH"}. This reflects the <b>storage</b>
 *       unit of the returned long, <i>not</i> the truncation granularity. A common user
 *       mistake is to declare the MV column as {@code 1:DAYS:EPOCH} after
 *       {@code date_trunc('DAY', ts)}; that silently scales values by 86.4M.</li>
 *   <li>Inferred granularity: {@code "1:<TimeUnit>"} where the truncation unit is mapped to its
 *       {@link TimeUnit} equivalent (e.g. {@code 'DAY' -> 1:DAYS}, {@code 'HOUR' -> 1:HOURS}).
 *       Truncation units that have no {@link TimeUnit} representation (WEEK, MONTH, QUARTER, YEAR)
 *       are rejected in v1 because the MV {@code DateTimeFieldSpec} granularity field is parsed
 *       through {@link TimeUnit#valueOf} and cannot represent them.</li>
 *   <li>Base format must be {@code 1:<UNIT>:EPOCH}. {@code SIMPLE_DATE_FORMAT} bases must
 *       route through {@code dateTimeConvert} first.</li>
 * </ul>
 */
public final class DateTruncRule implements TimeFunctionRule {

  public static final String CANONICAL_NAME = "datetrunc";
  private static final String DEFAULT_INPUT_TIME_UNIT = TimeUnit.MILLISECONDS.name();

  /**
   * Maps {@code date_trunc} singular unit tokens to their {@link TimeUnit} plural equivalents.
   * Truncation units without a {@link TimeUnit} mapping (WEEK, MONTH, QUARTER, YEAR) are absent
   * here and produce an explicit error below — they cannot be encoded into a Pinot
   * {@link DateTimeFieldSpec} granularity, which is parsed via {@link TimeUnit#valueOf}.
   */
  private static final Map<String, String> UNIT_TO_TIMEUNIT = Map.ofEntries(
      Map.entry("MILLISECOND", TimeUnit.MILLISECONDS.name()),
      Map.entry("SECOND", TimeUnit.SECONDS.name()),
      Map.entry("MINUTE", TimeUnit.MINUTES.name()),
      Map.entry("HOUR", TimeUnit.HOURS.name()),
      Map.entry("DAY", TimeUnit.DAYS.name()));

  @Override
  public String canonicalName() {
    return CANONICAL_NAME;
  }

  @Override
  public boolean acceptsOperandCount(int count) {
    return count >= 2 && count <= 5;
  }

  @Override
  public InferredTimeSpec infer(Function func, String baseTimeColumnName, DateTimeFieldSpec baseTimeFieldSpec) {
    List<Expression> operands = func.getOperands();
    String op = func.getOperator();

    String unit = TimeExprInferrer.requireStringLiteral(operands.get(0), "unit", op);
    TimeExprInferrer.requireIdentifierEqualsBaseTimeCol(operands.get(1), baseTimeColumnName, op);
    String baseUnit = TimeExprInferrer.requireUnitaryEpochBaseFormat(baseTimeFieldSpec, op);

    String inputTimeUnit = operands.size() >= 3
        ? TimeExprInferrer.requireStringLiteral(operands.get(2), "inputTimeUnit", op)
        : DEFAULT_INPUT_TIME_UNIT;
    // When inputTimeUnit is omitted, Pinot's date_trunc defaults it to MILLISECONDS — NOT to the
    // base column's unit. If the base is not MILLISECONDS this silently reinterprets timestamps
    // and yields wrong results. Require explicit match.
    Preconditions.checkState(inputTimeUnit.equalsIgnoreCase(baseUnit),
        "date_trunc inputTimeUnit '%s' does not match base time column '%s' unit '%s'. "
            + "Pinot's date_trunc defaults inputTimeUnit to MILLISECONDS when omitted; "
            + "either pass inputTimeUnit='%s' explicitly, or use dateTimeConvert.",
        inputTimeUnit, baseTimeColumnName, baseUnit, baseUnit);

    if (operands.size() >= 4) {
      TimeExprInferrer.requireStringLiteral(operands.get(3), "timeZone", op);
    }
    String outputTimeUnit = operands.size() == 5
        ? TimeExprInferrer.requireStringLiteral(operands.get(4), "outputTimeUnit", op)
        : inputTimeUnit;

    String inferredFormat = "1:" + outputTimeUnit.toUpperCase(Locale.ROOT) + ":EPOCH";
    String unitUpper = unit.toUpperCase(Locale.ROOT);
    String granularityUnit = UNIT_TO_TIMEUNIT.get(unitUpper);
    Preconditions.checkState(granularityUnit != null,
        "date_trunc unit '%s' has no representable Pinot DateTimeFieldSpec granularity in v1. "
            + "Supported units are %s. WEEK/MONTH/QUARTER/YEAR truncations cannot be expressed as "
            + "a TimeUnit-based granularity and must be implemented via dateTimeConvert with an "
            + "explicit bucket.",
        unit, UNIT_TO_TIMEUNIT.keySet());
    String inferredGranularity = "1:" + granularityUnit;

    String reason = "date_trunc unit='" + unit + "' => granularity '" + inferredGranularity
        + "'; outputTimeUnit='" + outputTimeUnit + "' (storage unit of the returned long) => format '"
        + inferredFormat + "' (note: format reflects storage unit, not granularity)";
    return new InferredTimeSpec(FormatMatcher.exact(inferredFormat), inferredGranularity, reason);
  }
}
