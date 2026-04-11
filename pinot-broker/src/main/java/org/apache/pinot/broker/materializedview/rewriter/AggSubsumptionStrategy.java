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
package org.apache.pinot.broker.materializedview.rewriter;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.broker.materializedview.MvMatchResult;
import org.apache.pinot.broker.materializedview.MvMetadataCache;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;


/**
 * Subsumption strategy for <b>aggregation</b> queries where the user's SELECT
 * list is a subset of the MV's pre-computed aggregation results.
 *
 * <p>This strategy handles queries with GROUP BY and/or aggregation functions.
 * The MV must have the same GROUP BY granularity as the user query, and the
 * user's requested aggregation expressions must be a subset of what the MV
 * has pre-computed.
 *
 * <p><b>TODO:</b> This is a stub implementation. The {@code acceptsShape} method
 * always returns {@code false}, so this strategy never matches. Full implementation
 * will be added in a follow-up change covering:
 * <ul>
 *   <li>Aggregation expression subset matching with alias-insensitive comparison</li>
 *   <li>GROUP BY exact equality enforcement</li>
 *   <li>Residual WHERE validation (only GROUP BY columns allowed)</li>
 *   <li>HAVING expression column mapping</li>
 *   <li>SELECT list rewrite with column name mapping (aggregation expr &rarr; MV column)</li>
 * </ul>
 */
public class AggSubsumptionStrategy extends AbstractSubsumptionStrategy {

  /**
   * Stub: always returns {@code false} to disable this strategy until
   * the full implementation is ready.
   */
  @Override
  protected boolean acceptsShape(PinotQuery userQuery, PinotQuery mvQuery) {
    // TODO: enable when implementation is complete
    //  return MvQueryShape.classify(userQuery) == MvQueryShape.AGGREGATION
    //      && MvQueryShape.classify(mvQuery) == MvQueryShape.AGGREGATION;
    return false;
  }

  @Override
  protected boolean groupByMatches(PinotQuery userQuery, PinotQuery mvQuery) {
    // TODO: require exact GROUP BY equality
    return false;
  }

  @Override
  protected boolean projectionSubsumes(List<Expression> userSelectList,
      Map<Expression, String> mvProjectionMap) {
    // TODO: check user aggregation expressions are a subset of MV's
    return false;
  }

  @Override
  protected boolean validateResidual(@Nullable Expression residualFilter, PinotQuery mvQuery) {
    // TODO: only GROUP BY columns may appear in residual filter
    return false;
  }

  @Override
  protected boolean orderByCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap) {
    // TODO: ORDER BY columns must exist in MV projection
    return false;
  }

  @Override
  protected boolean havingCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap) {
    // TODO: HAVING aggregation expressions must exist in MV projection
    return false;
  }

  @Override
  protected MvMatchResult buildResult(PinotQuery userQuery, MvMetadataCache.MvCacheEntry candidateEntry,
      @Nullable Expression residualFilter, Map<Expression, String> mvProjectionMap, boolean filtersEqual) {
    // TODO: rewrite SELECT with column name mapping
    return null;
  }
}
