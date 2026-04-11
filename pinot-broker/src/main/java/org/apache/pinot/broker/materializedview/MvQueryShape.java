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
 * <p>This enum is intentionally coarse-grained. New shapes (e.g. {@code DISTINCT},
 * {@code WINDOW}) can be added as the MV matching system evolves.
 */
public enum MvQueryShape {

  /** No aggregation functions and no GROUP BY clause. */
  SCAN,

  /** Contains aggregation functions in SELECT and/or a GROUP BY clause. */
  AGGREGATION,

  /** Query shapes not yet supported by any subsumption strategy. */
  UNSUPPORTED;

  /**
   * Determines the structural shape of the given query.
   *
   * <p>A query is classified as {@link #AGGREGATION} if it has a GROUP BY list
   * or if any top-level SELECT expression (after stripping aliases) is a recognized
   * aggregation function. Otherwise it is classified as {@link #SCAN}.
   *
   * @param query the compiled PinotQuery to classify
   * @return the query shape
   */
  public static MvQueryShape classify(PinotQuery query) {
    if (query.isSetGroupByList()) {
      return AGGREGATION;
    }

    List<Expression> selectList = query.getSelectList();
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

    // Check nested operands (e.g. transform functions wrapping aggregation)
    for (Expression operand : func.getOperands()) {
      if (containsAggregation(operand)) {
        return true;
      }
    }
    return false;
  }
}
