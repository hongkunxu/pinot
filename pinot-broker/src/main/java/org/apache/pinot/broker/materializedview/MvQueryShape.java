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
package org.apache.pinot.broker.materializedview;

import java.util.List;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/**
 * Classifies a {@link PinotQuery} into a high-level structural shape so that
 * subsumption strategies can quickly determine whether they are applicable.
 *
 * <p>Shapes that are not yet handled by any strategy are given explicit enum
 * values (e.g. {@code DISTINCT}) rather than falling through to {@code SCAN}
 * or {@code AGGREGATION}, preventing silent mismatches.
 */
public enum MvQueryShape {

  /** No aggregation functions, no GROUP BY, no DISTINCT. */
  SCAN,

  /** Contains aggregation functions in SELECT and/or a GROUP BY clause. */
  AGGREGATION,

  /**
   * {@code SELECT DISTINCT ...} query. CalciteSqlParser compiles this as
   * {@code SELECT distinct(col1, col2, ...)} — a single function-call
   * expression wrapping the select list.
   */
  DISTINCT,

  /** Query shapes not yet supported by any subsumption strategy. */
  UNSUPPORTED;

  /**
   * Determines the structural shape of the given query.
   *
   * @param query the compiled PinotQuery to classify
   * @return the query shape
   */
  public static MvQueryShape classify(PinotQuery query) {
    List<Expression> selectList = query.getSelectList();

    // DISTINCT detection: CalciteSqlParser wraps SELECT DISTINCT as a single
    // distinct(...) function call containing all projected columns.
    if (selectList != null && selectList.size() == 1) {
      Function func = selectList.get(0).getFunctionCall();
      if (func != null && "distinct".equals(func.getOperator())) {
        return DISTINCT;
      }
    }

    if (query.isSetGroupByList()) {
      return AGGREGATION;
    }

    if (selectList != null) {
      for (Expression expr : selectList) {
        if (containsAggregation(expr)) {
          return AGGREGATION;
        }
      }
    }

    return SCAN;
  }

  /**
   * Recursively checks whether an expression contains an aggregation function call.
   * Aliases ({@code as(expr, name)}) are transparently unwrapped.
   */
  private static boolean containsAggregation(Expression expr) {
    if (expr.getType() != ExpressionType.FUNCTION) {
      return false;
    }
    Function func = expr.getFunctionCall();
    if (func == null) {
      return false;
    }
    String operator = func.getOperator();

    if ("as".equals(operator)) {
      return containsAggregation(func.getOperands().get(0));
    }

    if (AggregationFunctionType.isAggregationFunction(operator)) {
      return true;
    }

    for (Expression operand : func.getOperands()) {
      if (containsAggregation(operand)) {
        return true;
      }
    }
    return false;
  }
}
