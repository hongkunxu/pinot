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

import org.apache.pinot.common.request.Function;
import org.apache.pinot.spi.data.DateTimeFieldSpec;


/**
 * Rule for inferring the output {@link DateTimeFieldSpec} format (and, when possible,
 * granularity) of a single whitelisted time-conversion function applied to the base-table
 * time column.
 *
 * <p>Each rule is keyed by the canonical function name (lowercase, underscores removed —
 * see {@link org.apache.pinot.common.function.FunctionRegistry#canonicalize}).
 *
 * <p>Implementations MUST:
 * <ul>
 *   <li>Enforce that the first operand is a bare identifier equal to the base time column.
 *       Nested function calls on the first operand are rejected in v1.</li>
 *   <li>Require every semantic parameter (format strings, units, granularity, pattern,
 *       timezone) to be a string literal. Non-literal arguments are rejected because the
 *       inferrer needs static values to produce a deterministic expected format.</li>
 *   <li>Throw {@link IllegalStateException} with a user-facing, actionable message on any
 *       violation. The caller surfaces the message directly to the REST client at MV
 *       create/update time.</li>
 * </ul>
 */
public interface TimeFunctionRule {

  /**
   * Canonical function name (e.g. {@code "datetimeconvert"}). Must match
   * {@link org.apache.pinot.common.function.FunctionRegistry#canonicalize} so that SQL like
   * {@code dateTimeConvert(...)} and {@code date_time_convert(...)} both resolve to the same
   * rule.
   */
  String canonicalName();

  /**
   * Returns {@code true} when the given operand count is valid for this function. The caller
   * invokes this before {@link #infer}; rules may still throw inside {@link #infer} if they
   * discover a finer-grained arity violation.
   */
  boolean acceptsOperandCount(int count);

  /**
   * Infers the output time spec produced by {@code func} applied to the base time column.
   *
   * @param func                 the parsed function call (already unwrapped from the AS alias)
   * @param baseTimeColumnName   the base table's {@code segmentsConfig.timeColumnName}
   * @param baseTimeFieldSpec    the base table's {@link DateTimeFieldSpec} for that column
   * @return inferred spec — never {@code null}
   * @throws IllegalStateException with a user-facing message on any validation failure
   */
  InferredTimeSpec infer(Function func, String baseTimeColumnName, DateTimeFieldSpec baseTimeFieldSpec);
}
