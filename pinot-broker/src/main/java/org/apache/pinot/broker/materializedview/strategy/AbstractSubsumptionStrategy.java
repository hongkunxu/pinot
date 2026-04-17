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
import org.apache.pinot.broker.materializedview.MvMatchUtils;
import org.apache.pinot.broker.materializedview.MvMetadataCache;
import org.apache.pinot.broker.materializedview.MvQueryShape;
import org.apache.pinot.broker.materializedview.MvRewritePlan;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.PinotQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
 * <p>Strategies produce plan fragments — {@link MvRewritePlan} instances with
 * {@code execMode = null}. The
 * {@link org.apache.pinot.broker.materializedview.MvQueryRewriteEngine MvQueryRewriteEngine} resolves execution
 * mode and split compatibility after the strategy returns.
 *
 * <p>Implementations should be stateless and thread-safe.
 *
 * @see ExactSubsumptionStrategy
 * @see ScanSubsumptionStrategy
 * @see AggregationSubsumptionStrategy
 */
public abstract class AbstractSubsumptionStrategy implements MvMatchStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSubsumptionStrategy.class);

  /**
   * Template method that drives the subsumption matching pipeline.
   *
   * <p>This method is {@code final} to enforce a consistent matching order
   * across all strategies. Subclasses customize behavior through the
   * {@code protected abstract} hook methods.
   */
  @Nullable
  @Override
  public final MvRewritePlan match(PinotQuery userQuery, MvMetadataCache.MvCacheEntry candidateEntry) {
    PinotQuery mvQuery = candidateEntry.getCompiledQuery();
    String strategyName = getClass().getSimpleName();
    String mvName = candidateEntry.getMvTableNameWithType();

    if (mvQuery == null) {
      LOGGER.debug("MV match [{}] strategy={}: compiled query is null", mvName, strategyName);
      return null;
    }

    // Step 1: shape gate
    if (!acceptsShape(userQuery, mvQuery)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at SHAPE_GATE (userShape={}, mvShape={})",
          mvName, strategyName, MvQueryShape.classify(userQuery), MvQueryShape.classify(mvQuery));
      return null;
    }

    // Step 2: GROUP BY
    if (!groupByMatches(userQuery, mvQuery)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at GROUP_BY", mvName, strategyName);
      return null;
    }

    // Step 3: projection subsumption
    Map<Expression, String> mvProjectionMap = MvMatchUtils.buildMvProjectionMap(mvQuery);
    if (!projectionSubsumes(userQuery.getSelectList(), mvProjectionMap)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at PROJECTION", mvName, strategyName);
      return null;
    }

    // Step 4: WHERE matching + residual extraction (shared logic)
    Expression userFilter = userQuery.getFilterExpression();
    Expression mvFilter = mvQuery.getFilterExpression();
    boolean filtersEqual = Objects.equals(userFilter, mvFilter);

    Expression residualFilter = null;
    if (!filtersEqual) {
      residualFilter = MvMatchUtils.tryExtractResidualFilter(userFilter, mvFilter);
      if (residualFilter == null && userFilter != null) {
        LOGGER.debug("MV match [{}] strategy={}: rejected at WHERE_FILTER "
            + "(user filter is not a superset of MV filter)", mvName, strategyName);
        return null;
      }
      if (userFilter == null) {
        LOGGER.debug("MV match [{}] strategy={}: rejected at WHERE_FILTER "
            + "(MV has filter but user query does not)", mvName, strategyName);
        return null;
      }
    }

    // Step 5: residual validation (subclass-specific)
    if (!validateResidual(residualFilter, mvQuery)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at RESIDUAL_VALIDATION", mvName, strategyName);
      return null;
    }

    // Step 6: ORDER BY
    if (!orderByCompatible(userQuery, mvQuery, mvProjectionMap)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at ORDER_BY", mvName, strategyName);
      return null;
    }

    // Step 7: HAVING
    if (!havingCompatible(userQuery, mvQuery, mvProjectionMap)) {
      LOGGER.debug("MV match [{}] strategy={}: rejected at HAVING", mvName, strategyName);
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
   * the MV's definition) is valid for this strategy.
   */
  protected abstract boolean validateResidual(@Nullable Expression residualFilter, PinotQuery mvQuery);

  /**
   * Returns {@code true} if the user query's ORDER BY clause can be satisfied
   * by the MV table.
   */
  protected abstract boolean orderByCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap);

  /**
   * Returns {@code true} if the user query's HAVING clause can be satisfied
   * by the MV table.
   */
  protected abstract boolean havingCompatible(PinotQuery userQuery, PinotQuery mvQuery,
      Map<Expression, String> mvProjectionMap);

  /**
   * Constructs the {@link MvRewritePlan} fragment containing the rewritten query,
   * match type, and cost score. The plan's execution mode is left unset (null)
   * — it is resolved by the engine after the strategy returns.
   *
   * @param userQuery       the original user query
   * @param candidateEntry  the matched MV cache entry
   * @param residualFilter  residual WHERE filter (null if none)
   * @param mvProjectionMap alias-stripped expression &rarr; MV column name
   * @param filtersEqual    true if user and MV filters are identical
   */
  protected abstract MvRewritePlan buildResult(PinotQuery userQuery,
      MvMetadataCache.MvCacheEntry candidateEntry, @Nullable Expression residualFilter,
      Map<Expression, String> mvProjectionMap, boolean filtersEqual);
}
