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
package org.apache.pinot.broker.materializedview.equivalence;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;


/**
 * Defines an equivalence transformation for re-aggregating a pre-computed MV
 * column when the MV has finer granularity than the user query.
 *
 * <p>Each implementation encapsulates one category of aggregation functions
 * that share the same re-aggregation pattern. For example, distributive
 * functions (SUM, MIN, MAX) can be re-aggregated with the same function,
 * while sketch-based functions (DISTINCTCOUNTHLL) require the MV to store
 * a raw sketch variant for merging.
 *
 * <p>Implementations must be stateless and thread-safe.
 *
 * @see PassthroughEquivalence
 * @see SketchMergeEquivalence
 * @see AggregationEquivalenceRegistry
 */
public interface AggregationEquivalence {

  /**
   * Returns {@code true} if this equivalence can be used in split-mode execution.
   *
   * <p>In split mode the base side returns intermediates for the original user
   * aggregation function, while the MV side returns intermediates for the
   * re-aggregation function. The broker reducer merges all DataTables using the
   * original function's reducer. If the two functions produce incompatible
   * intermediate types (e.g. COUNT produces LONG intermediates but SUM also
   * produces LONG — however their DataTable column types may differ), split mode
   * should be rejected.
   *
   * <p>An equivalence is split-safe when the re-aggregation function is identical
   * to the user function (distributive functions: SUM, MIN, MAX) and the MV
   * stores the same intermediate format. COUNT→SUM and sketch functions are NOT
   * split-safe.
   *
   * @return {@code true} if this equivalence produces split-safe intermediates
   */
  default boolean isSplitSafe() {
    return false;
  }

  /**
   * Returns {@code true} if this equivalence can handle the given combination
   * of user aggregation function and MV aggregation function.
   *
   * @param userFunctionName the aggregation function name from the user query
   *                         (uppercase, e.g. "SUM", "DISTINCTCOUNTHLL")
   * @param mvFunctionName   the aggregation function name from the MV definition
   *                         (uppercase, e.g. "SUM", "DISTINCTCOUNTRAWHLL")
   */
  boolean matches(String userFunctionName, String mvFunctionName);

  /**
   * Builds a re-aggregation expression that applies the appropriate function
   * on the pre-computed MV column.
   *
   * @param userAggExpression the original aggregation expression from the user
   *                          query (e.g. {@code SUM(revenue)})
   * @param mvColumnName      the MV column name that stores the pre-computed
   *                          value (e.g. {@code sum_rev})
   * @return the re-aggregation expression (e.g. {@code SUM(sum_rev)}), or
   *         {@code null} if the transformation cannot be applied
   */
  @Nullable
  Expression rewrite(Expression userAggExpression, String mvColumnName);

  /**
   * Extracts trailing LITERAL operands from the user aggregation expression.
   * These are configuration parameters (e.g. {@code log2m} for HLL functions)
   * that may have been injected by broker overrides such as
   * {@code handleHLLLog2mOverride} and should be carried over to the rewritten
   * expression.
   *
   * @param userAggExpression the user's aggregation expression
   * @return trailing literal operands (after the first operand), or empty list
   */
  default List<Expression> extractTrailingLiterals(Expression userAggExpression) {
    Function func = userAggExpression.getFunctionCall();
    if (func == null || func.getOperands() == null || func.getOperandsSize() <= 1) {
      return List.of();
    }
    List<Expression> trailing = new ArrayList<>();
    for (int i = 1; i < func.getOperands().size(); i++) {
      Expression op = func.getOperands().get(i);
      if (op.getType() == ExpressionType.LITERAL) {
        trailing.add(op);
      }
    }
    return trailing;
  }
}
