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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.broker.materializedview.MvMatchResult;
import org.apache.pinot.broker.materializedview.MvMatchUtils;
import org.apache.pinot.broker.materializedview.MvMetadataCache;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;


/**
 * Base class for materialized view matching strategies that follow the
 * <a href="https://en.wikipedia.org/wiki/Subsumption">subsumption</a> model:
 * a query can be answered by an MV if the MV's definition logically
 * <em>subsumes</em> (covers) the query's requirements.
 *
 * <p>This class implements the {@link MvMatchStrategy} interface using a
 * <b>template method</b> pattern. The overall matching flow is fixed:
 * <ol>
 *   <li>Shape gate — reject queries whose structural shape is incompatible</li>
 *   <li>GROUP BY check</li>
 *   <li>Projection (SELECT) subsumption check</li>
 *   <li>WHERE filter matching and residual extraction</li>
 *   <li>Residual filter validation</li>
 *   <li>ORDER BY compatibility</li>
 *   <li>HAVING compatibility</li>
 *   <li>Build the rewritten query and cost</li>
 * </ol>
 *
 * <p>Concrete subclasses customize individual steps by overriding the
 * {@code protected abstract} hook methods. This design allows adding new
 * matching strategies (scan subsumption, aggregation subsumption, etc.)
 * while reusing the shared matching infrastructure in {@link MvMatchUtils}.
 *
 * <p>Implementations should be stateless and thread-safe.
 *
 * @see ExactSubsumptionStrategy
 * @see ScanSubsumptionStrategy
 * @see AggregationSubsumptionStrategy
 */
public abstract class AbstractSubsumptionStrategy implements MvMatchStrategy {

  /**
   * Template method that drives the subsumption matching pipeline.
   *
   * <p>This method is {@code final} to enforce a consistent matching order
   * across all strategies. Subclasses customize behavior through the
   * {@code protected abstract} hook methods.
   */
  @Nullable
  @Override
  public final MvMatchResult match(PinotQuery userQuery, MvMetadataCache.MvCacheEntry candidateEntry) {
    PinotQuery mvQuery = candidateEntry.getCompiledQuery();
    if (mvQuery == null) {
      return null;
    }

    // Step 1: shape gate
    if (!acceptsShape(userQuery, mvQuery)) {
      return null;
    }

    // Step 2: GROUP BY
    if (!groupByMatches(userQuery, mvQuery)) {
      return null;
    }

    // Step 3: projection subsumption
    Map<Expression, String> mvProjectionMap = MvMatchUtils.buildMvProjectionMap(mvQuery);
    if (!projectionSubsumes(userQuery.getSelectList(), mvProjectionMap)) {
      return null;
    }

    // Step 4: WHERE matching + residual extraction (shared logic)
    Expression userFilter = userQuery.getFilterExpression();
    Expression mvFilter = mvQuery.getFilterExpression();
    boolean filtersEqual = Objects.equals(userFilter, mvFilter);

    Expression residualFilter = null;
    if (!filtersEqual) {
      residualFilter = MvMatchUtils.tryExtractResidualFilter(userFilter, mvFilter);
      // null residual when filters differ means user filter is not a superset of MV filter
      if (residualFilter == null && userFilter != null) {
        return null;
      }
      // user has no filter but MV has one — cannot match
      if (userFilter == null) {
        return null;
      }
    }

    // Step 5: residual validation (subclass-specific)
    if (!validateResidual(residualFilter, mvQuery)) {
      return null;
    }

    // Step 6: ORDER BY
    if (!orderByCompatible(userQuery, mvQuery, mvProjectionMap)) {
      return null;
    }

    // Step 7: HAVING
    if (!havingCompatible(userQuery, mvQuery, mvProjectionMap)) {
      return null;
    }

    // Step 8: build result
    return buildResult(userQuery, candidateEntry, residualFilter, mvProjectionMap, filtersEqual);
  }

  // -----------------------------------------------------------------------
  //  Hook methods — to be implemented by concrete strategies
  // -----------------------------------------------------------------------

  /**
   * Returns {@code true} if this strategy is applicable to the given
   * combination of user query shape and MV query shape.
   *
   * @param userQuery the user's compiled query
   * @param mvQuery   the MV's compiled defined-SQL query
   */
  protected abstract boolean acceptsShape(PinotQuery userQuery, PinotQuery mvQuery);

  /**
   * Returns {@code true} if the user query's GROUP BY clause is compatible
   * with the MV's GROUP BY clause.
   */
  protected abstract boolean groupByMatches(PinotQuery userQuery, PinotQuery mvQuery);

  /**
   * Returns {@code true} if the MV's projection (SELECT list) covers all
   * expressions required by the user query.
   *
   * @param userSelectList  the user query's SELECT expressions
   * @param mvProjectionMap alias-stripped expression &rarr; MV column name
   */
  protected abstract boolean projectionSubsumes(List<Expression> userSelectList,
      Map<Expression, String> mvProjectionMap);

  /**
   * Returns {@code true} if the residual filter (extra WHERE predicates beyond
   * the MV's definition) is valid for this strategy. For example, a scan
   * strategy requires that residual columns exist in the MV table.
   *
   * @param residualFilter the residual filter expression (may be null)
   * @param mvQuery        the MV's compiled query
   */
  protected abstract boolean validateResidual(@Nullable Expression residualFilter, PinotQuery mvQuery);

  /**
   * Returns {@code true} if the user query's ORDER BY clause can be satisfied
   * by the MV table.
   *
   * @param userQuery       the user's compiled query
   * @param mvQuery         the MV's compiled query
   * @param mvProjectionMap alias-stripped expression &rarr; MV column name
   */
  protected abstract boolean orderByCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap);

  /**
   * Returns {@code true} if the user query's HAVING clause can be satisfied
   * by the MV table.
   *
   * @param userQuery       the user's compiled query
   * @param mvQuery         the MV's compiled query
   * @param mvProjectionMap alias-stripped expression &rarr; MV column name
   */
  protected abstract boolean havingCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap);

  /**
   * Constructs the final {@link MvMatchResult} containing the rewritten query
   * and cost score.
   *
   * @param userQuery       the original user query
   * @param candidateEntry  the matched MV cache entry
   * @param residualFilter  residual WHERE filter (null if none)
   * @param mvProjectionMap alias-stripped expression &rarr; MV column name
   * @param filtersEqual    true if user and MV filters are identical
   */
  protected abstract MvMatchResult buildResult(PinotQuery userQuery,
      MvMetadataCache.MvCacheEntry candidateEntry, @Nullable Expression residualFilter,
      Map<Expression, String> mvProjectionMap, boolean filtersEqual);
}
