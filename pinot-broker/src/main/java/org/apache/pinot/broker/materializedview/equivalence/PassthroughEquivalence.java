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
import org.apache.pinot.common.utils.request.RequestUtils;


/**
 * Equivalence for aggregation functions whose MV expression is identical to
 * the user expression (after stripping aliases), but whose re-aggregation
 * function may differ from the original.
 *
 * <p>This covers two categories:
 * <ul>
 *   <li><b>Distributive</b> — the re-aggregation function is the same as the
 *       user function: {@code SUM(col) -> SUM(mv_sum_col)},
 *       {@code MIN(col) -> MIN(mv_min_col)},
 *       {@code MAX(col) -> MAX(mv_max_col)}</li>
 *   <li><b>Algebraic (simple transformation)</b> — the re-aggregation function
 *       differs: {@code COUNT(col) -> SUM(mv_count_col)},
 *       {@code COUNT(*) -> SUM(mv_count_star_col)}</li>
 * </ul>
 *
 * <p>The {@link #matches} method requires the MV to store the same aggregation
 * function as the user query (e.g. MV has {@code SUM(revenue)} for a user
 * query with {@code SUM(revenue)}).
 *
 * <p>Trailing literal parameters injected by broker overrides (e.g. HLL
 * {@code log2m}) are preserved in the rewritten expression.
 *
 * <p>This class is stateless and thread-safe.
 */
public class PassthroughEquivalence implements AggregationEquivalence {

  private final String _userFunctionName;
  private final String _reAggFunctionName;

  /**
   * @param userFunctionName the user-side aggregation function name (uppercase)
   * @param reAggFunctionName the function to apply during re-aggregation on
   *                          the MV column (uppercase)
   */
  public PassthroughEquivalence(String userFunctionName, String reAggFunctionName) {
    _userFunctionName = userFunctionName;
    _reAggFunctionName = reAggFunctionName;
  }

  @Override
  public boolean matches(String userFunctionName, String mvFunctionName) {
    return _userFunctionName.equalsIgnoreCase(userFunctionName)
        && _userFunctionName.equalsIgnoreCase(mvFunctionName);
  }

  @Nullable
  @Override
  public Expression rewrite(Expression userAggExpression, String mvColumnName) {
    List<Expression> trailingLiterals = extractTrailingLiterals(userAggExpression);
    List<Expression> operands = new ArrayList<>(1 + trailingLiterals.size());
    operands.add(RequestUtils.getIdentifierExpression(mvColumnName));
    operands.addAll(trailingLiterals);
    return RequestUtils.getFunctionExpression(_reAggFunctionName.toLowerCase(),
        operands.toArray(new Expression[0]));
  }
}
