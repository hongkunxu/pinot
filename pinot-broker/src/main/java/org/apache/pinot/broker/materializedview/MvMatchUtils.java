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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.FilterKind;


/**
 * Shared utility methods for materialized view subsumption matching strategies.
 *
 * <p>All methods are stateless and thread-safe.
 */
public final class MvMatchUtils {

  private MvMatchUtils() {
  }

  // -----------------------------------------------------------------------
  //  Alias handling
  // -----------------------------------------------------------------------

  /**
   * Strips the top-level {@code AS} alias from an expression if present.
   * For {@code as(expr, alias)} returns {@code expr}; otherwise returns the
   * expression unchanged.
   *
   * @param expr the expression to unwrap
   * @return the inner expression without the alias wrapper
   */
  public static Expression stripAlias(Expression expr) {
    if (expr.getType() == ExpressionType.FUNCTION) {
      Function func = expr.getFunctionCall();
      if (func != null && "as".equals(func.getOperator())) {
        return func.getOperands().get(0);
      }
    }
    return expr;
  }

  /**
   * Extracts the output column name that a SELECT expression maps to in the
   * MV table schema.
   *
   * <ul>
   *   <li>{@code as(expr, alias)} &rarr; alias identifier name</li>
   *   <li>bare identifier &rarr; identifier name</li>
   *   <li>anything else &rarr; throws {@link IllegalStateException}</li>
   * </ul>
   *
   * @param expr a SELECT-list expression from the MV's compiled query
   * @return the MV table column name
   */
  public static String extractMvColumnName(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func != null && "as".equals(func.getOperator())) {
      Expression aliasExpr = func.getOperands().get(1);
      return aliasExpr.getIdentifier().getName();
    }
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      return expr.getIdentifier().getName();
    }
    throw new IllegalStateException(
        "Cannot extract MV column name from expression: " + RequestUtils.prettyPrint(expr));
  }

  /**
   * Builds a mapping from alias-stripped expressions to MV table column names
   * for all entries in the MV's SELECT list.
   *
   * <p>Example: for {@code SELECT city, SUM(revenue) AS sum_rev FROM ...}
   * the returned map is:
   * <pre>
   *   Identifier("city")                      &rarr; "city"
   *   Function("SUM", [Identifier("revenue")]) &rarr; "sum_rev"
   * </pre>
   *
   * @param mvQuery the MV's compiled PinotQuery
   * @return map from stripped expression to MV column name
   */
  public static Map<Expression, String> buildMvProjectionMap(PinotQuery mvQuery) {
    List<Expression> selectList = mvQuery.getSelectList();
    if (selectList == null) {
      return Map.of();
    }
    Map<Expression, String> map = new HashMap<>(selectList.size());
    for (Expression expr : selectList) {
      Expression stripped = stripAlias(expr);
      String columnName = extractMvColumnName(expr);
      map.put(stripped, columnName);
    }
    return map;
  }

  /**
   * Extracts the user-specified alias name from an expression, if present.
   * Returns {@code null} for expressions without an alias.
   *
   * @param expr a SELECT-list expression from the user's query
   * @return the alias name, or {@code null} if the expression has no alias
   */
  @Nullable
  public static String extractUserAlias(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func != null && "as".equals(func.getOperator())) {
      return func.getOperands().get(1).getIdentifier().getName();
    }
    return null;
  }

  /**
   * Rewrites the user query's SELECT list by replacing each expression with
   * its corresponding MV table column name, while preserving the user's
   * original alias if one was specified.
   *
   * <p>For example, given:
   * <pre>
   *   MV defined:  SELECT city, SUM(revenue) AS sum_rev FROM orders GROUP BY city
   *   User query:  SELECT city, SUM(revenue) AS r_sum  FROM orders GROUP BY city
   * </pre>
   * the rewritten SELECT list will be:
   * <pre>
   *   SELECT city, sum_rev AS r_sum FROM mv_table GROUP BY city
   * </pre>
   *
   * @param userSelectList  the user query's original SELECT expressions
   * @param mvProjectionMap alias-stripped expression &rarr; MV column name
   * @return the rewritten SELECT expression list
   */
  public static List<Expression> rewriteSelectList(List<Expression> userSelectList,
      Map<Expression, String> mvProjectionMap) {
    List<Expression> rewritten = new ArrayList<>(userSelectList.size());
    for (Expression expr : userSelectList) {
      Expression stripped = stripAlias(expr);
      String mvColumnName = mvProjectionMap.get(stripped);
      Expression mvColExpr = RequestUtils.getIdentifierExpression(mvColumnName);

      String userAlias = extractUserAlias(expr);
      if (userAlias != null) {
        mvColExpr = RequestUtils.getFunctionExpression("as", mvColExpr,
            RequestUtils.getIdentifierExpression(userAlias));
      }
      rewritten.add(mvColExpr);
    }
    return rewritten;
  }

  // -----------------------------------------------------------------------
  //  WHERE / residual filter
  // -----------------------------------------------------------------------

  /**
   * Checks whether the user's WHERE filter is an AND-superset of the MV's
   * WHERE filter and extracts the residual predicates.
   *
   * <p>Rules:
   * <ul>
   *   <li>MV has no filter, user has a filter &rarr; entire user filter is residual</li>
   *   <li>User has no filter, MV has a filter &rarr; not a superset, returns null</li>
   *   <li>User conjuncts contain all MV conjuncts &rarr; difference is residual</li>
   *   <li>Otherwise &rarr; returns null (no match)</li>
   * </ul>
   *
   * @param userFilter the user query's filter expression (may be null)
   * @param mvFilter   the MV query's filter expression (may be null)
   * @return the residual filter expression, or {@code null} if the user filter
   *         is not a valid superset of the MV filter
   */
  @Nullable
  public static Expression tryExtractResidualFilter(@Nullable Expression userFilter,
      @Nullable Expression mvFilter) {
    if (mvFilter == null && userFilter != null) {
      return userFilter;
    }
    if (userFilter == null) {
      return null;
    }

    Set<Expression> userConjuncts = flattenAnd(userFilter);
    Set<Expression> mvConjuncts = flattenAnd(mvFilter);

    if (!userConjuncts.containsAll(mvConjuncts)) {
      return null;
    }

    Set<Expression> residual = new HashSet<>(userConjuncts);
    residual.removeAll(mvConjuncts);

    if (residual.isEmpty()) {
      return null;
    }

    return buildAndExpression(new ArrayList<>(residual));
  }

  /**
   * Flattens a filter expression into a set of AND conjuncts. If the top-level
   * operator is AND, it is recursively unwrapped; otherwise the entire expression
   * is treated as a single conjunct.
   */
  public static Set<Expression> flattenAnd(Expression filter) {
    Set<Expression> conjuncts = new HashSet<>();
    collectAndConjuncts(filter, conjuncts);
    return conjuncts;
  }

  private static void collectAndConjuncts(Expression expr, Set<Expression> conjuncts) {
    if (expr.getType() == ExpressionType.FUNCTION) {
      Function function = expr.getFunctionCall();
      if (function != null && FilterKind.AND.name().equals(function.getOperator())) {
        for (Expression operand : function.getOperands()) {
          collectAndConjuncts(operand, conjuncts);
        }
        return;
      }
    }
    conjuncts.add(expr);
  }

  /**
   * Builds an AND expression from the given list of operands. Returns the
   * single operand directly when the list has exactly one element.
   */
  public static Expression buildAndExpression(List<Expression> operands) {
    if (operands.size() == 1) {
      return operands.get(0);
    }
    return RequestUtils.getFunctionExpression(FilterKind.AND.name(), operands);
  }

  // -----------------------------------------------------------------------
  //  Expression remapping
  // -----------------------------------------------------------------------

  /**
   * Recursively rewrites an expression tree by replacing any sub-expression
   * found in {@code mvProjectionMap} with a simple identifier referencing the
   * corresponding MV column name.
   *
   * <p>This is used to transform HAVING, ORDER BY, and residual WHERE
   * expressions from base-table semantics to MV-table semantics. For example,
   * {@code SUM(revenue) > 1000} becomes {@code sum_rev > 1000} when the
   * projection map contains {@code SUM(revenue) -> "sum_rev"}.
   *
   * @param expr            the expression to remap
   * @param mvProjectionMap alias-stripped expression &rarr; MV column name
   * @return the remapped expression (may be the same instance if nothing changed)
   */
  public static Expression remapExpression(Expression expr, Map<Expression, String> mvProjectionMap) {
    if (mvProjectionMap.containsKey(expr)) {
      return RequestUtils.getIdentifierExpression(mvProjectionMap.get(expr));
    }

    if (expr.getType() == ExpressionType.FUNCTION) {
      Function func = expr.getFunctionCall();
      if (func != null && func.getOperands() != null) {
        List<Expression> originalOperands = func.getOperands();
        List<Expression> remapped = new ArrayList<>(originalOperands.size());
        boolean changed = false;
        for (Expression operand : originalOperands) {
          Expression result = remapExpression(operand, mvProjectionMap);
          remapped.add(result);
          if (result != operand) {
            changed = true;
          }
        }
        if (changed) {
          return RequestUtils.getFunctionExpression(func.getOperator(), remapped);
        }
      }
    }

    return expr;
  }

  /**
   * Checks whether all non-literal leaf sub-expressions within {@code expr}
   * can be resolved via the given projection map. Identifiers and aggregation
   * functions must appear as keys; literals and comparison/logical operators
   * are traversed transparently.
   *
   * @param expr            the expression to validate
   * @param mvProjectionMap alias-stripped expression &rarr; MV column name
   * @return {@code true} if every resolvable leaf is present in the map
   */
  public static boolean allReferencesResolvable(Expression expr, Map<Expression, String> mvProjectionMap) {
    if (mvProjectionMap.containsKey(expr)) {
      return true;
    }
    if (expr.getType() == ExpressionType.LITERAL) {
      return true;
    }
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      return false;
    }
    if (expr.getType() == ExpressionType.FUNCTION) {
      Function func = expr.getFunctionCall();
      if (func != null && func.getOperands() != null) {
        for (Expression operand : func.getOperands()) {
          if (!allReferencesResolvable(operand, mvProjectionMap)) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  // -----------------------------------------------------------------------
  //  Column reference collection
  // -----------------------------------------------------------------------

  /**
   * Collects all column identifiers referenced by the given expression.
   * Useful for verifying that a residual filter or ORDER BY clause only
   * references columns available in the MV table.
   *
   * @param expr the expression to scan
   * @return set of column name strings
   */
  public static Set<String> collectReferencedColumns(Expression expr) {
    Set<String> columns = new HashSet<>();
    collectColumnsRecursive(expr, columns);
    return columns;
  }

  private static void collectColumnsRecursive(Expression expr, Set<String> columns) {
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      String name = expr.getIdentifier().getName();
      if (!"*".equals(name)) {
        columns.add(name);
      }
      return;
    }
    if (expr.getType() == ExpressionType.FUNCTION) {
      Function func = expr.getFunctionCall();
      if (func != null && func.getOperands() != null) {
        for (Expression operand : func.getOperands()) {
          collectColumnsRecursive(operand, columns);
        }
      }
    }
  }
}
