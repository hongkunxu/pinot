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
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.broker.materializedview.MatchType;
import org.apache.pinot.broker.materializedview.MvMatchUtils;
import org.apache.pinot.broker.materializedview.MvMetadataCache;
import org.apache.pinot.broker.materializedview.MvRewritePlan;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;


/**
 * Subsumption strategy that requires an <b>exact structural match</b> between
 * the user query and the MV definition (after stripping aliases and ignoring
 * column order in the SELECT list).
 *
 * <p>This is the tightest form of subsumption: the query's SELECT, WHERE,
 * GROUP BY, ORDER BY, and HAVING must all be semantically identical to the
 * MV's definition. No residual WHERE filter is allowed — if the WHERE clauses
 * differ in any way, the match fails.
 *
 * <p>The rewritten query maps all SELECT expressions to MV table column names
 * while preserving the user's original aliases.
 *
 * <p>Cost: {@code 0.0} (perfect match, highest priority).
 *
 * <p>This strategy accepts any query shape (SCAN or AGGREGATION) because
 * exact matching is valid regardless of whether aggregation is present.
 */
public class ExactSubsumptionStrategy extends AbstractSubsumptionStrategy {

  private static final double COST_EXACT = 0.0;

  /**
   * Accepts any query shape — exact matching is universally applicable.
   */
  @Override
  protected boolean acceptsShape(PinotQuery userQuery, PinotQuery mvQuery) {
    return true;
  }

  /**
   * Requires GROUP BY lists to be identical (same expressions, same order).
   */
  @Override
  protected boolean groupByMatches(PinotQuery userQuery, PinotQuery mvQuery) {
    List<Expression> userList = userQuery.getGroupByList();
    List<Expression> mvList = mvQuery.getGroupByList();
    if (userList == null && mvList == null) {
      return true;
    }
    if (userList == null || mvList == null) {
      return false;
    }
    return userList.equals(mvList);
  }

  /**
   * Requires SELECT lists to contain exactly the same expressions (order-insensitive,
   * alias-insensitive). The stripped expression sets must be equal.
   */
  @Override
  protected boolean projectionSubsumes(List<Expression> userSelectList,
      Map<Expression, String> mvProjectionMap) {
    if (userSelectList == null) {
      return mvProjectionMap.isEmpty();
    }
    if (userSelectList.size() != mvProjectionMap.size()) {
      return false;
    }
    Set<Expression> userStripped = new HashSet<>(userSelectList.size());
    for (Expression expr : userSelectList) {
      userStripped.add(MvMatchUtils.stripAlias(expr));
    }
    return userStripped.equals(mvProjectionMap.keySet());
  }

  /**
   * Rejects any residual filter — exact match requires WHERE clauses to be
   * identical between the user query and the MV definition.
   */
  @Override
  protected boolean validateResidual(@Nullable Expression residualFilter, PinotQuery mvQuery) {
    return residualFilter == null;
  }

  /**
   * Requires ORDER BY lists to be identical between user query and MV query.
   */
  @Override
  protected boolean orderByCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap) {
    List<Expression> userList = userQuery.getOrderByList();
    List<Expression> mvList = mvQuery.getOrderByList();
    if (userList == null && mvList == null) {
      return true;
    }
    if (userList == null || mvList == null) {
      return false;
    }
    return userList.equals(mvList);
  }

  /**
   * Requires HAVING expressions to be identical between user query and MV query.
   */
  @Override
  protected boolean havingCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap) {
    return Objects.equals(userQuery.getHavingExpression(), mvQuery.getHavingExpression());
  }

  /**
   * Builds the rewritten query by swapping the table name to the MV table
   * and mapping each SELECT expression to its MV column name while preserving
   * the user's original alias.
   *
   * <p>The strategy no longer checks split-mode compatibility here — that
   * concern is handled by {@code MvQueryRewriteEngine.resolvePlan}.
   */
  @Override
  protected MvRewritePlan buildResult(PinotQuery userQuery, MvMetadataCache.MvCacheEntry candidateEntry,
      @Nullable Expression residualFilter, Map<Expression, String> mvProjectionMap, boolean filtersEqual) {
    PinotQuery rewritten = userQuery.deepCopy();
    rewritten.getDataSource().setTableName(candidateEntry.getMvTableNameWithType());
    rewritten.setSelectList(MvMatchUtils.rewriteSelectList(userQuery.getSelectList(), mvProjectionMap));
    rewritten.setFilterExpression(null);
    // Remap GROUP BY and ORDER BY from user column names to MV column names.
    if (userQuery.getGroupByList() != null) {
      List<Expression> remappedGroupBy = new ArrayList<>(userQuery.getGroupByList().size());
      for (Expression expr : userQuery.getGroupByList()) {
        remappedGroupBy.add(MvMatchUtils.remapExpression(expr, mvProjectionMap));
      }
      rewritten.setGroupByList(remappedGroupBy);
    }
    if (userQuery.getOrderByList() != null) {
      List<Expression> remappedOrderBy = new ArrayList<>(userQuery.getOrderByList().size());
      for (Expression expr : userQuery.getOrderByList()) {
        remappedOrderBy.add(MvMatchUtils.remapExpression(expr, mvProjectionMap));
      }
      rewritten.setOrderByList(remappedOrderBy);
    }
    return new MvRewritePlan(candidateEntry.getMvTableNameWithType(),
        MatchType.EXACT, null, rewritten, null, COST_EXACT);
  }
}
