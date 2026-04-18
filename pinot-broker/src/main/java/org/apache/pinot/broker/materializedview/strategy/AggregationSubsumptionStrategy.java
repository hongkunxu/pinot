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
package org.apache.pinot.broker.materializedview.strategy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.broker.materializedview.MatchType;
import org.apache.pinot.broker.materializedview.MvMatchUtils;
import org.apache.pinot.broker.materializedview.MvMetadataCache;
import org.apache.pinot.broker.materializedview.MvQueryShape;
import org.apache.pinot.broker.materializedview.MvRewritePlan;
import org.apache.pinot.broker.materializedview.equivalence.AggregationEquivalence;
import org.apache.pinot.broker.materializedview.equivalence.AggregationEquivalenceRegistry;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.sql.parsers.CalciteSqlParser;


/**
 * Subsumption strategy for <b>aggregation</b> queries.
 *
 * <p>The rewritten query always retains GROUP BY and wraps aggregation
 * columns with re-aggregation functions via
 * {@link AggregationEquivalenceRegistry} (e.g. {@code SUM(col)} on the user
 * query becomes {@code SUM(sum_col)} on the MV). When the MV's GROUP BY
 * granularity matches the user query exactly, each group contains a single
 * pre-computed row, so the re-aggregation is effectively a no-op — but it
 * guarantees that the server produces an aggregation intermediate DataTable,
 * which is critical for split-mode merging where both sides must use the
 * same schema.
 *
 * <p>Cost model:
 * <ul>
 *   <li>{@code 6.0} — re-aggregation without residual WHERE</li>
 *   <li>{@code 7.0} — re-aggregation with residual WHERE</li>
 * </ul>
 *
 * <p>Implementations should be stateless and thread-safe.
 */
public class AggregationSubsumptionStrategy extends AbstractSubsumptionStrategy {

  private static final double COST_REAGG = 6.0;
  private static final double COST_REAGG_WITH_RESIDUAL = 7.0;

  @Override
  protected boolean acceptsShape(PinotQuery userQuery, PinotQuery mvQuery) {
    return MvQueryShape.classify(userQuery) == MvQueryShape.AGGREGATION
        && MvQueryShape.classify(mvQuery) == MvQueryShape.AGGREGATION;
  }

  @Override
  protected boolean groupByMatches(PinotQuery userQuery, PinotQuery mvQuery) {
    List<Expression> userGroupBy = userQuery.getGroupByList();
    List<Expression> mvGroupBy = mvQuery.getGroupByList();

    if (userGroupBy == null || userGroupBy.isEmpty()
        || mvGroupBy == null || mvGroupBy.isEmpty()) {
      return false;
    }

    Set<Expression> userSet = new HashSet<>(userGroupBy);
    Set<Expression> mvSet = new HashSet<>(mvGroupBy);
    return mvSet.containsAll(userSet);
  }

  @Override
  protected boolean projectionSubsumes(List<Expression> userSelectList,
      Map<Expression, String> mvProjectionMap) {
    if (userSelectList == null || userSelectList.isEmpty()) {
      return false;
    }
    for (Expression expr : userSelectList) {
      Expression stripped = MvMatchUtils.stripAlias(expr);
      // Plain column reference: direct MV projection hit is sufficient.
      if (stripped.getFunctionCall() == null) {
        if (!mvProjectionMap.containsKey(stripped)) {
          return false;
        }
        continue;
      }
      // Aggregate function: an exact projection match is NOT enough on its own. We need an
      // AggregationEquivalence rule to re-aggregate the pre-computed MV column correctly.
      // Without a rule we would fall back to a bare column reference (e.g. AVG(revenue) →
      // avg_rev), which produces wrong results for non-distributive functions.
      if (findEquivalentMvEntry(stripped, mvProjectionMap) == null) {
        return false;
      }
    }
    return true;
  }

  @Nullable
  private static Object[] findEquivalentMvEntry(Expression userExpr,
      Map<Expression, String> mvProjectionMap) {
    Function userFunc = userExpr.getFunctionCall();
    if (userFunc == null) {
      return null;
    }
    String userFuncName = userFunc.getOperator();
    List<Expression> userOperands = userFunc.getOperands();

    for (Map.Entry<Expression, String> mvEntry : mvProjectionMap.entrySet()) {
      Expression mvExpr = mvEntry.getKey();
      Function mvFunc = mvExpr.getFunctionCall();
      if (mvFunc == null) {
        continue;
      }
      if (!operandsMatch(userOperands, mvFunc.getOperands())) {
        continue;
      }
      AggregationEquivalence rule =
          AggregationEquivalenceRegistry.findRule(userFuncName, mvFunc.getOperator());
      if (rule != null) {
        return new Object[]{mvEntry.getValue(), rule};
      }
    }
    return null;
  }

  private static boolean operandsMatch(@Nullable List<Expression> a, @Nullable List<Expression> b) {
    if (a == null && b == null) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    // MV definitions are authored without broker-injected config literals (e.g. log2m for HLL).
    // The broker may append trailing literals to the user query at runtime. We tolerate those
    // by comparing only up to the MV's operand count — but only when the extra operands in the
    // user query are all literals (so PERCENTILE(col,50) vs PERCENTILE(col,99) still rejects,
    // because the MV also carries the literal 99 as a semantically significant operand).
    int mvSize = b.size();
    int userSize = a.size();
    if (userSize < mvSize) {
      return false;
    }
    // All operands up to mvSize must match exactly.
    for (int i = 0; i < mvSize; i++) {
      if (!a.get(i).equals(b.get(i))) {
        return false;
      }
    }
    // Extra user operands (beyond the MV operand count) must all be literals —
    // otherwise this is a semantically distinct call, not just a config injection.
    for (int i = mvSize; i < userSize; i++) {
      if (a.get(i).getType() != ExpressionType.LITERAL) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean validateResidual(@Nullable Expression residualFilter, PinotQuery mvQuery) {
    if (residualFilter == null) {
      return true;
    }

    List<Expression> mvGroupBy = mvQuery.getGroupByList();
    if (mvGroupBy == null || mvGroupBy.isEmpty()) {
      return false;
    }

    Set<String> groupByColumnNames = new HashSet<>(mvGroupBy.size());
    for (Expression gbExpr : mvGroupBy) {
      groupByColumnNames.addAll(MvMatchUtils.collectReferencedColumns(gbExpr));
    }

    Set<String> residualColumns = MvMatchUtils.collectReferencedColumns(residualFilter);
    return groupByColumnNames.containsAll(residualColumns);
  }

  @Override
  protected boolean orderByCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap) {
    List<Expression> orderByList = userQuery.getOrderByList();
    if (orderByList == null || orderByList.isEmpty()) {
      return true;
    }
    for (Expression orderByExpr : orderByList) {
      Expression inner = CalciteSqlParser.removeOrderByFunctions(orderByExpr);
      if (!isResolvable(inner, mvProjectionMap)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean havingCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap) {
    Expression having = userQuery.getHavingExpression();
    if (having == null) {
      return true;
    }
    return allReferencesResolvableWithEquivalence(having, mvProjectionMap);
  }

  private boolean allReferencesResolvableWithEquivalence(Expression expr,
      Map<Expression, String> mvProjectionMap) {
    if (isResolvable(expr, mvProjectionMap)) {
      return true;
    }
    if (expr.getType() == ExpressionType.LITERAL) {
      return true;
    }
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      return false;
    }
    if (expr.getFunctionCall() != null && expr.getFunctionCall().getOperands() != null) {
      for (Expression operand : expr.getFunctionCall().getOperands()) {
        if (!allReferencesResolvableWithEquivalence(operand, mvProjectionMap)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  private static boolean isResolvable(Expression expr, Map<Expression, String> mvProjectionMap) {
    if (mvProjectionMap.containsKey(expr)) {
      return true;
    }
    return findEquivalentMvEntry(expr, mvProjectionMap) != null;
  }

  @Override
  protected MvRewritePlan buildResult(PinotQuery userQuery, MvMetadataCache.MvCacheEntry candidateEntry,
      @Nullable Expression residualFilter, Map<Expression, String> mvProjectionMap, boolean filtersEqual) {
    return buildReAggResult(userQuery, candidateEntry, residualFilter, mvProjectionMap, filtersEqual);
  }

  private MvRewritePlan buildReAggResult(PinotQuery userQuery, MvMetadataCache.MvCacheEntry candidateEntry,
      @Nullable Expression residualFilter, Map<Expression, String> mvProjectionMap, boolean filtersEqual) {
    PinotQuery rewritten = userQuery.deepCopy();
    rewritten.getDataSource().setTableName(candidateEntry.getMvTableNameWithType());

    rewritten.setSelectList(
        buildReAggSelectList(userQuery.getSelectList(), mvProjectionMap));

    List<Expression> userGroupBy = userQuery.getGroupByList();
    List<Expression> remappedGroupBy = new ArrayList<>(userGroupBy.size());
    for (Expression gbExpr : userGroupBy) {
      String mvCol = mvProjectionMap.get(gbExpr);
      remappedGroupBy.add(RequestUtils.getIdentifierExpression(mvCol));
    }
    rewritten.setGroupByList(remappedGroupBy);

    Expression originalHaving = userQuery.getHavingExpression();
    if (originalHaving != null) {
      rewritten.setHavingExpression(
          remapExpressionWithEquivalence(originalHaving, mvProjectionMap));
    } else {
      rewritten.setHavingExpression(null);
    }

    Expression remappedResidual = null;
    if (residualFilter != null) {
      remappedResidual = MvMatchUtils.remapExpression(residualFilter, mvProjectionMap);
    }
    rewritten.setFilterExpression(remappedResidual);

    if (userQuery.getOrderByList() != null && !userQuery.getOrderByList().isEmpty()) {
      List<Expression> remappedOrderBy = new ArrayList<>(userQuery.getOrderByList().size());
      for (Expression orderByExpr : userQuery.getOrderByList()) {
        remappedOrderBy.add(remapExpressionWithEquivalence(orderByExpr, mvProjectionMap));
      }
      rewritten.setOrderByList(remappedOrderBy);
    }

    double cost = filtersEqual ? COST_REAGG : COST_REAGG_WITH_RESIDUAL;
    return new MvRewritePlan(candidateEntry.getMvTableNameWithType(),
        MatchType.AGG_REAGG, null, rewritten, null, cost);
  }

  private List<Expression> buildReAggSelectList(List<Expression> userSelectList,
      Map<Expression, String> mvProjectionMap) {
    List<Expression> result = new ArrayList<>(userSelectList.size());
    for (Expression expr : userSelectList) {
      Expression stripped = MvMatchUtils.stripAlias(expr);
      String userAlias = MvMatchUtils.extractUserAlias(expr);
      Expression rewritten;

      if (mvProjectionMap.containsKey(stripped)
          && stripped.getFunctionCall() == null) {
        rewritten = RequestUtils.getIdentifierExpression(mvProjectionMap.get(stripped));
      } else {
        rewritten = rewriteAggregationExpression(stripped, mvProjectionMap);
      }

      if (userAlias != null) {
        rewritten = RequestUtils.getFunctionExpression("as", rewritten,
            RequestUtils.getIdentifierExpression(userAlias));
      }
      result.add(rewritten);
    }
    return result;
  }

  private Expression rewriteAggregationExpression(Expression stripped,
      Map<Expression, String> mvProjectionMap) {
    Object[] match = findEquivalentMvEntry(stripped, mvProjectionMap);
    if (match != null) {
      String mvCol = (String) match[0];
      AggregationEquivalence rule = (AggregationEquivalence) match[1];
      return rule.rewrite(stripped, mvCol);
    }

    throw new IllegalStateException(
        "Cannot rewrite aggregation expression: " + stripped);
  }

  private Expression remapExpressionWithEquivalence(Expression expr,
      Map<Expression, String> mvProjectionMap) {
    if (mvProjectionMap.containsKey(expr) && expr.getFunctionCall() == null) {
      return RequestUtils.getIdentifierExpression(mvProjectionMap.get(expr));
    }

    if (expr.getFunctionCall() != null) {
      if (mvProjectionMap.containsKey(expr)) {
        return rewriteAggregationExpression(expr, mvProjectionMap);
      }
      Object[] match = findEquivalentMvEntry(expr, mvProjectionMap);
      if (match != null) {
        return ((AggregationEquivalence) match[1]).rewrite(expr, (String) match[0]);
      }
    }

    if (expr.getFunctionCall() != null && expr.getFunctionCall().getOperands() != null) {
      Function func = expr.getFunctionCall();
      List<Expression> original = func.getOperands();
      List<Expression> remapped = new ArrayList<>(original.size());
      boolean changed = false;
      for (Expression operand : original) {
        Expression result = remapExpressionWithEquivalence(operand, mvProjectionMap);
        remapped.add(result);
        if (result != operand) {
          changed = true;
        }
      }
      if (changed) {
        return RequestUtils.getFunctionExpression(func.getOperator(), remapped);
      }
    }

    return expr;
  }
}
