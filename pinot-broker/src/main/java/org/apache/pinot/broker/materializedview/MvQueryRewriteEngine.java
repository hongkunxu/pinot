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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.broker.materializedview.strategy.MvMatchStrategy;
import org.apache.pinot.common.minion.MvDefinitionMetadata;
import org.apache.pinot.common.minion.MvFreshness;
import org.apache.pinot.common.request.PinotQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Orchestrates materialized view query rewriting in two phases:
 *
 * <ol>
 *   <li><b>Static matching</b> — strategies produce {@link MvRewritePlan}
 *       fragments (rewritten MV query + match type + cost) without considering
 *       runtime state.</li>
 *   <li><b>Runtime gating and plan resolution</b> — freshness is checked
 *       before matching; after matching, execution mode is determined and
 *       split-compatibility is verified.</li>
 * </ol>
 *
 * <p>For each candidate MV, strategies are tried in registration order (highest
 * precision first). Once a strategy produces a plan that also passes runtime
 * compatibility checks ({@link #resolvePlan}), no lower-precision strategies
 * are attempted for the same MV. If a strategy matches structurally but its
 * plan is rejected by runtime checks (e.g. EXACT match incompatible with
 * split-mode GROUP BY), the engine falls through to the next strategy.
 * The overall best plan across all candidates is then selected by lowest cost.
 *
 * <p>Thread-safety: this class is immutable after construction and safe to share.
 */
public class MvQueryRewriteEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(MvQueryRewriteEngine.class);

  private final MvMetadataCache _mvMetadataCache;
  private final List<MvMatchStrategy> _strategies;

  public MvQueryRewriteEngine(MvMetadataCache mvMetadataCache, List<MvMatchStrategy> strategies) {
    _mvMetadataCache = mvMetadataCache;
    _strategies = List.copyOf(strategies);
  }

  /**
   * Attempts to rewrite the given query to use a materialized view.
   *
   * @param pinotQuery       the compiled user query
   * @param rawBaseTableName the raw (no type suffix) name of the base table being queried
   * @return the rewrite result containing candidate names and optional plan,
   *         or {@code null} if no candidate MVs exist for the base table
   */
  @Nullable
  public MvRewriteResult tryRewrite(PinotQuery pinotQuery, String rawBaseTableName) {
    List<MvMetadataCache.MvCacheEntry> candidates = _mvMetadataCache.getMvEntriesForBaseTable(rawBaseTableName);
    if (candidates == null || candidates.isEmpty()) {
      return null;
    }

    List<String> candidateNames = new ArrayList<>(candidates.size());
    for (MvMetadataCache.MvCacheEntry candidate : candidates) {
      candidateNames.add(candidate.getMvTableNameWithType());
    }

    MvRewritePlan bestPlan = null;
    String bestStrategyName = null;

    for (MvMetadataCache.MvCacheEntry candidate : candidates) {
      if (!isEligible(candidate)) {
        continue;
      }

      for (MvMatchStrategy strategy : _strategies) {
        try {
          MvRewritePlan plan = strategy.match(pinotQuery, candidate);
          if (plan == null) {
            continue;
          }

          plan = resolvePlan(plan, candidate, pinotQuery);
          if (plan == null) {
            // Strategy matched structurally but the plan is incompatible with
            // the required execution mode (e.g. EXACT + split + GROUP BY).
            // Try lower-precision strategies for this candidate.
            continue;
          }

          if (bestPlan == null || plan.compareTo(bestPlan) < 0) {
            bestPlan = plan;
            bestStrategyName = strategy.getClass().getSimpleName();
          }
          // A fully resolved plan was found; skip lower-precision strategies.
          break;
        } catch (Exception e) {
          LOGGER.warn("Strategy {} failed for MV {}", strategy.getClass().getSimpleName(),
              candidate.getMvTableNameWithType(), e);
        }
      }
    }

    if (bestPlan != null) {
      LOGGER.info("MV rewrite succeeded for table [{}]: strategy={}, mvTable={}, "
              + "matchType={}, execMode={}, cost={}, originalQuery=[{}], rewrittenQuery=[{}]",
          rawBaseTableName, bestStrategyName, bestPlan.getMvTableNameWithType(),
          bestPlan.getMatchType(), bestPlan.getExecMode(), bestPlan.getCost(),
          pinotQuery, bestPlan.getMvQuery());
    } else {
      LOGGER.info("MV rewrite miss for table [{}]: evaluated {} candidate(s)={}, "
              + "queryShape={}, userQuery=[{}]",
          rawBaseTableName, candidates.size(), candidateNames,
          MvQueryShape.classify(pinotQuery), pinotQuery);
    }

    return new MvRewriteResult(candidateNames, bestPlan);
  }

  // -----------------------------------------------------------------------
  //  Runtime gating — separated from AST matching
  // -----------------------------------------------------------------------

  /**
   * Checks whether a candidate MV is eligible for rewrite based on runtime
   * state (freshness). This is intentionally separate from AST matching.
   */
  private static boolean isEligible(MvMetadataCache.MvCacheEntry candidate) {
    MvFreshness freshness = candidate.getFreshness();
    if (freshness != MvFreshness.FRESH) {
      LOGGER.info("MV skip [{}]: freshness={} (requires FRESH)",
          candidate.getMvTableNameWithType(), freshness);
      return false;
    }
    return true;
  }

  // -----------------------------------------------------------------------
  //  Execution mode resolution — separated from AST matching
  // -----------------------------------------------------------------------

  /**
   * Resolves the execution mode for a plan fragment produced by a strategy,
   * and checks split-mode compatibility.
   *
   * <p>The decision is driven by whether the MV definition includes a
   * {@link MvDefinitionMetadata.MvSplitSpec}:
   * <ul>
   *   <li><b>No split spec</b> (batch MV): the MV is assumed to cover all base
   *       table data. Returns {@link ExecutionMode#FULL_REWRITE}.</li>
   *   <li><b>Has split spec</b> (incremental MV): the MV covers only historical
   *       data. {@code coverageUpperMs} must be &gt; 0 (i.e. at least one APPEND
   *       has successfully completed) for the MV to be queryable. If coverage is
   *       not yet confirmed, returns {@code null} to skip this MV.</li>
   * </ul>
   *
   * <p>In split mode, EXACT match is rejected when the user query has GROUP BY
   * because it replaces aggregation functions with plain MV column references,
   * producing incompatible DataTable schemas during merge.
   *
   * @return the resolved plan with execution mode set, or {@code null} if the
   *         plan is incompatible with the required execution mode
   */
  @Nullable
  private static MvRewritePlan resolvePlan(MvRewritePlan plan, MvMetadataCache.MvCacheEntry candidate,
      PinotQuery userQuery) {
    MvDefinitionMetadata.MvSplitSpec splitSpec = candidate.getSplitSpec();

    if (splitSpec != null) {
      long coverageUpperMs = candidate.getCoverageUpperMs();
      if (coverageUpperMs <= 0) {
        // Incremental MV has no confirmed coverage yet (cold-start: generator
        // initialized the watermark but no APPEND has completed). Skip this MV
        // to avoid split queries against an empty MV table.
        LOGGER.info("MV skip [{}]: cold-start, no coverage confirmed yet "
                + "(watermark initialized but no APPEND completed), matchType={}",
            candidate.getMvTableNameWithType(), plan.getMatchType());
        return null;
      }

      // EXACT match replaces aggregation functions (e.g. SUM(col)) with plain
      // MV column references (e.g. col_sum), producing physical column types
      // on the MV side that are incompatible with the base table's aggregation
      // intermediate types during split-mode merge. Reject and let
      // AggregationSubsumptionStrategy handle the query instead.
      if (userQuery.isSetGroupByList() && plan.getMatchType() == MatchType.EXACT) {
        LOGGER.info("MV skip [{}]: EXACT match rejected in split mode with GROUP BY "
                + "(schema incompatibility during merge, falling back to AGG_REAGG)",
            candidate.getMvTableNameWithType());
        return null;
      }

      PinotQuery baseQueryTemplate = userQuery.deepCopy();
      return plan.withExecMode(ExecutionMode.SPLIT_REWRITE, baseQueryTemplate,
          splitSpec, coverageUpperMs);
    }

    return plan.withExecMode(ExecutionMode.FULL_REWRITE, null, null, 0);
  }
}
