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
import org.apache.pinot.broker.materializedview.rewriter.MvMatchStrategy;
import org.apache.pinot.common.request.PinotQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Orchestrates materialized view query rewriting by evaluating each candidate
 * MV against registered {@link MvMatchStrategy} implementations and selecting
 * the best match (lowest cost) across all candidates.
 *
 * <p>For each candidate MV, strategies are tried in registration order (highest
 * precision first). Once a strategy matches, that MV's best result is determined
 * and no lower-precision strategies are attempted for the same MV. The overall
 * best result across all candidates is then selected by lowest cost.
 *
 * <p>Strategies must be registered in descending precision order (e.g.
 * {@link ExactSubsumptionStrategy} before {@link ScanSubsumptionStrategy}
 * before {@link AggSubsumptionStrategy}).
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
   * @return the rewrite result containing candidate names and optional match,
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
      candidateNames.add(candidate.getMetadata().getMvTableNameWithType());
    }

    MvMatchResult bestResult = null;
    String bestStrategyName = null;

    // Outer loop: iterate over candidate MVs.
    // Inner loop: for each MV, try strategies in registration order (highest precision first).
    // Once a strategy matches a given MV, skip remaining strategies for that MV since
    // the first match is already the highest-precision result for that candidate.
    // The overall best result across all MVs is selected by lowest cost.
    for (MvMetadataCache.MvCacheEntry candidate : candidates) {
      for (MvMatchStrategy strategy : _strategies) {
        try {
          MvMatchResult result = strategy.match(pinotQuery, candidate);
          if (result != null) {
            if (bestResult == null || result.compareTo(bestResult) < 0) {
              bestResult = result;
              bestStrategyName = strategy.getClass().getSimpleName();
            }
            break;
          }
        } catch (Exception e) {
          LOGGER.warn("Strategy {} failed for MV {}", strategy.getClass().getSimpleName(),
              candidate.getMetadata().getMvTableNameWithType(), e);
        }
      }
    }

    if (bestResult != null) {
      LOGGER.warn("MV rewrite succeeded for table [{}]: strategy={}, mvTable={}, cost={}, "
              + "originalQuery=[{}], rewrittenQuery=[{}]",
          rawBaseTableName, bestStrategyName, bestResult.getMvTableName(), bestResult.getCost(),
          pinotQuery, bestResult.getRewrittenQuery());
    }

    return new MvRewriteResult(candidateNames, bestResult);
  }
}
