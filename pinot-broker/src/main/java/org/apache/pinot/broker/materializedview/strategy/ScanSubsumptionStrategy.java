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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.broker.materializedview.MatchType;
import org.apache.pinot.broker.materializedview.MvMatchUtils;
import org.apache.pinot.broker.materializedview.MvMetadataCache;
import org.apache.pinot.broker.materializedview.MvQueryShape;
import org.apache.pinot.broker.materializedview.MvRewritePlan;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;


/**
 * Subsumption strategy for <b>non-aggregation (scan)</b> queries where the
 * user's SELECT list is a subset of the MV's SELECT list.
 *
 * <p>Both the user query and the MV must have {@link MvQueryShape#SCAN} shape
 * (no aggregation functions, no GROUP BY). The MV table is treated as a
 * pre-filtered / pre-projected physical table whose columns correspond to the
 * columns listed in the MV's {@code definedSql}.
 *
 * <p>Cost model:
 * <ul>
 *   <li>{@code 2.0} — projection-subset match without residual WHERE</li>
 *   <li>{@code 3.0} — projection-subset match with residual WHERE</li>
 * </ul>
 */
public class ScanSubsumptionStrategy extends AbstractSubsumptionStrategy {

  private static final double COST_SCAN_SUBSUMPTION = 2.0;
  private static final double COST_SCAN_WITH_RESIDUAL = 3.0;

  @Override
  protected boolean acceptsShape(PinotQuery userQuery, PinotQuery mvQuery) {
    return MvQueryShape.classify(userQuery) == MvQueryShape.SCAN
        && MvQueryShape.classify(mvQuery) == MvQueryShape.SCAN;
  }

  @Override
  protected boolean groupByMatches(PinotQuery userQuery, PinotQuery mvQuery) {
    return !userQuery.isSetGroupByList() && !mvQuery.isSetGroupByList();
  }

  @Override
  protected boolean projectionSubsumes(List<Expression> userSelectList,
      Map<Expression, String> mvProjectionMap) {
    if (userSelectList == null || userSelectList.isEmpty()) {
      return false;
    }
    for (Expression expr : userSelectList) {
      Expression stripped = MvMatchUtils.stripAlias(expr);
      if (!mvProjectionMap.containsKey(stripped)) {
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
    Set<String> residualColumns = MvMatchUtils.collectReferencedColumns(residualFilter);
    Collection<String> mvColumns = MvMatchUtils.buildMvProjectionMap(mvQuery).values();
    return mvColumns.containsAll(residualColumns);
  }

  @Override
  protected boolean orderByCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap) {
    List<Expression> orderByList = userQuery.getOrderByList();
    if (orderByList == null || orderByList.isEmpty()) {
      return true;
    }
    Set<String> mvColumnNames = Set.copyOf(mvProjectionMap.values());
    for (Expression orderByExpr : orderByList) {
      Set<String> referencedColumns = MvMatchUtils.collectReferencedColumns(orderByExpr);
      if (!mvColumnNames.containsAll(referencedColumns)) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean havingCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap) {
    return userQuery.getHavingExpression() == null;
  }

  @Override
  protected MvRewritePlan buildResult(PinotQuery userQuery, MvMetadataCache.MvCacheEntry candidateEntry,
      @Nullable Expression residualFilter, Map<Expression, String> mvProjectionMap, boolean filtersEqual) {
    PinotQuery rewritten = userQuery.deepCopy();
    rewritten.getDataSource().setTableName(candidateEntry.getMvTableNameWithType());
    rewritten.setSelectList(MvMatchUtils.rewriteSelectList(userQuery.getSelectList(), mvProjectionMap));
    // Remap residual filter columns to MV column names in case the MV uses aliased columns.
    Expression remappedResidual = residualFilter != null
        ? MvMatchUtils.remapExpression(residualFilter, mvProjectionMap) : null;
    rewritten.setFilterExpression(remappedResidual);

    double cost = filtersEqual ? COST_SCAN_SUBSUMPTION : COST_SCAN_WITH_RESIDUAL;
    return new MvRewritePlan(candidateEntry.getMvTableNameWithType(),
        MatchType.SCAN_SUBSUME, null, rewritten, null, cost);
  }
}
