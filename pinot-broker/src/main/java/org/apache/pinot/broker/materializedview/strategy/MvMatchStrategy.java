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

import javax.annotation.Nullable;
import org.apache.pinot.broker.materializedview.MvMetadataCache;
import org.apache.pinot.broker.materializedview.MvRewritePlan;
import org.apache.pinot.common.request.PinotQuery;


/**
 * Strategy interface for matching a user query against a candidate materialized view.
 *
 * <p>Implementations produce an {@link MvRewritePlan} fragment containing the
 * rewritten MV query, match type, and cost. Execution mode and base query
 * template are resolved later by
 * {@link org.apache.pinot.broker.materializedview.MvQueryRewriteEngine MvQueryRewriteEngine}.
 *
 * <p>Implementations should be stateless and thread-safe. New matching algorithms
 * (partial aggregation, column projection, time-range rollup, etc.) can be added
 * by implementing this interface and registering with {@code MvQueryRewriteEngine}.
 */
public interface MvMatchStrategy {

  /**
   * Attempts to match the given user query against the candidate MV entry.
   *
   * @param userQuery      the compiled PinotQuery from the user's SQL
   * @param candidateEntry the cached MV entry (metadata + pre-compiled definedSql query)
   * @return an {@link MvRewritePlan} fragment if the MV can answer the user query,
   *         {@code null} otherwise
   */
  @Nullable
  MvRewritePlan match(PinotQuery userQuery, MvMetadataCache.MvCacheEntry candidateEntry);
}
