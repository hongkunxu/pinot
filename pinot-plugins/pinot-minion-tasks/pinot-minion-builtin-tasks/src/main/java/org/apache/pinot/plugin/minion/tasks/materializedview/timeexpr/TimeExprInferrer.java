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

import com.google.common.base.Preconditions;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.rule.IdentityRule;
import org.apache.pinot.spi.data.DateTimeFieldSpec;


/**
 * Inference entry point for MV time-column SELECT expressions.
 *
 * <p>v1 only validates the MV's primary time column
 * ({@code segmentsConfig.timeColumnName}). The inferrer enforces a strict whitelist of
 * forms:
 * <ul>
 *   <li>{@link ExpressionType#IDENTIFIER identity} — the bare base time column;</li>
 *   <li>{@link ExpressionType#FUNCTION single function call} to one of
 *       {@link TimeFunctionRegistry#supportedCanonicalNames()}.</li>
 * </ul>
 * Everything else — arithmetic, unsupported functions, nested function calls, literal-only
 * expressions — is rejected with an actionable message.
 *
 * <p><b>Explicit non-goals (v1):</b>
 * <ul>
 *   <li>{@code toDateTime}'s output granularity is not inferred (the function carries no
 *       bucket semantic). The MV {@link DateTimeFieldSpec#getGranularity()} is still
 *       required to be non-empty, but is not further constrained.</li>
 *   <li>{@code date_trunc} and {@code toDateTime} reject base columns whose format is
 *       {@code SIMPLE_DATE_FORMAT}; users must route through {@code dateTimeConvert}
 *       first.</li>
 *   <li>Nested function calls (e.g. {@code dateTimeConvert(date_trunc(ts, 'DAY'), ...)})
 *       are rejected in v1 — every rule requires the first operand to be a bare
 *       identifier equal to the base time column.</li>
 * </ul>
 *
 * <p>TODO(mv): Extend to every MV {@code DateTimeFieldSpec} column, not just the primary
 * time column. The same inferrer is reusable; only the "first arg must be the primary base
 * time column" constraint loosens to "first arg must be some base dateTime column", with
 * the stricter constraint retained in analyzer Step 6.
 */
public final class TimeExprInferrer {

  private TimeExprInferrer() {
  }

  /**
   * Infers the output time spec of {@code sourceExpr}.
   *
   * @param sourceExpr           the MV SELECT expression producing the MV time column
   *                             (alias already stripped)
   * @param baseTimeColumnName   base table's primary time column name
   * @param baseTimeFieldSpec    base table's {@link DateTimeFieldSpec} for that column
   * @throws IllegalStateException with a user-facing message on any violation
   */
  public static InferredTimeSpec infer(Expression sourceExpr, String baseTimeColumnName,
      DateTimeFieldSpec baseTimeFieldSpec) {
    Preconditions.checkNotNull(sourceExpr, "sourceExpr");
    Preconditions.checkNotNull(baseTimeColumnName, "baseTimeColumnName");
    Preconditions.checkNotNull(baseTimeFieldSpec, "baseTimeFieldSpec");

    ExpressionType exprType = sourceExpr.getType();
    if (exprType == ExpressionType.IDENTIFIER) {
      return IdentityRule.INSTANCE.inferIdentity(sourceExpr, baseTimeColumnName, baseTimeFieldSpec);
    }

    if (exprType == ExpressionType.FUNCTION) {
      Function func = sourceExpr.getFunctionCall();
      Preconditions.checkState(func != null,
          "MV time column expression is a FUNCTION but has no function call payload");
      String canonical = FunctionRegistry.canonicalize(func.getOperator());
      TimeFunctionRule rule = TimeFunctionRegistry.get(canonical);
      Preconditions.checkState(rule != null,
          "MV time column expression uses unsupported function '%s'. Supported functions: %s. "
              + "Alternatively, use the base time column directly without any transformation.",
          func.getOperator(), TimeFunctionRegistry.supportedCanonicalNames());
      Preconditions.checkState(rule.acceptsOperandCount(func.getOperandsSize()),
          "Function '%s' called with %s arguments in the MV time column expression is not supported. "
              + "See Pinot documentation for the accepted argument counts.",
          func.getOperator(), func.getOperandsSize());
      return rule.infer(func, baseTimeColumnName, baseTimeFieldSpec);
    }

    throw new IllegalStateException(
        "MV time column expression must be either the base time column '" + baseTimeColumnName
            + "' or a single call to one of " + TimeFunctionRegistry.supportedCanonicalNames()
            + ". Got expression type: " + exprType);
  }

  // ----- Shared validation helpers used by rules ---------------------------------------

  /**
   * Requires {@code expr} to be a bare identifier equal to {@code baseTimeColumnName}.
   * Used by every rule for operand[0].
   */
  public static void requireIdentifierEqualsBaseTimeCol(Expression expr, String baseTimeColumnName,
      String functionName) {
    Preconditions.checkState(expr.getType() == ExpressionType.IDENTIFIER,
        "Function '%s' first argument must be the base time column '%s' (a bare identifier, "
            + "not a nested expression).",
        functionName, baseTimeColumnName);
    String actual = expr.getIdentifier().getName();
    Preconditions.checkState(baseTimeColumnName.equals(actual),
        "Function '%s' first argument must be the base time column '%s', got '%s'.",
        functionName, baseTimeColumnName, actual);
  }

  /**
   * Requires {@code expr} to be a string literal. Returns the literal's string value.
   * Numeric/boolean/null literals are rejected because a format/unit/granularity/pattern
   * must be statically known as a string.
   */
  public static String requireStringLiteral(Expression expr, String argName, String functionName) {
    Preconditions.checkState(expr.getType() == ExpressionType.LITERAL,
        "Function '%s' argument '%s' must be a string literal.", functionName, argName);
    Literal literal = expr.getLiteral();
    Preconditions.checkState(literal != null && literal.isSetStringValue(),
        "Function '%s' argument '%s' must be a string literal.", functionName, argName);
    return literal.getStringValue();
  }

  /**
   * Requires the base time column's format to be a unitary EPOCH format ({@code 1:<UNIT>:EPOCH}).
   * Used by rules that cannot consume a {@code SIMPLE_DATE_FORMAT} base.
   *
   * @return the time unit token (e.g. {@code MILLISECONDS}) on success
   */
  public static String requireUnitaryEpochBaseFormat(DateTimeFieldSpec baseTimeFieldSpec, String functionName) {
    String fmt = baseTimeFieldSpec.getFormat();
    String[] parts = fmt == null ? new String[0] : fmt.split(":");
    Preconditions.checkState(parts.length >= 3 && "1".equals(parts[0]) && "EPOCH".equalsIgnoreCase(parts[2]),
        "Function '%s' requires the base time column to use a unitary EPOCH format of the form "
            + "'1:<UNIT>:EPOCH' (got '%s'). Route through dateTimeConvert if the base column uses "
            + "SIMPLE_DATE_FORMAT or a non-unitary size.",
        functionName, fmt);
    return parts[1];
  }
}
