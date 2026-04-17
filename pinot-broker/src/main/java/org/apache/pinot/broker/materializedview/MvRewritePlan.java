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

import javax.annotation.Nullable;
import org.apache.pinot.common.minion.MvDefinitionMetadata.MvSplitSpec;
import org.apache.pinot.common.request.PinotQuery;


/**
 * Structured output of the MV rewrite layer, separating static matching
 * ({@link MatchType}) from runtime execution decisions ({@link ExecutionMode}).
 *
 * <p>Strategies produce plan fragments with {@code execMode = null} and
 * {@code baseQueryTemplate = null}. The {@link MvQueryRewriteEngine} then
 * resolves execution mode and populates these fields via
 * {@link #withExecMode(ExecutionMode, PinotQuery, MvSplitSpec, long)}.
 *
 * <p>Lower cost is better. The rewrite engine picks the plan with the lowest
 * cost when multiple MVs match a single user query.
 */
public class MvRewritePlan implements Comparable<MvRewritePlan> {

  private final String _mvTableNameWithType;
  private final MatchType _matchType;
  @Nullable
  private final ExecutionMode _execMode;
  private final PinotQuery _mvQuery;
  @Nullable
  private final PinotQuery _baseQueryTemplate;
  private final double _cost;

  // Split-mode runtime fields — populated by the engine during plan resolution,
  // not by strategies. Only meaningful when execMode == SPLIT_REWRITE.
  @Nullable
  private final MvSplitSpec _splitSpec;
  private final long _coverageUpperMs;

  public MvRewritePlan(String mvTableNameWithType, MatchType matchType,
      @Nullable ExecutionMode execMode, PinotQuery mvQuery,
      @Nullable PinotQuery baseQueryTemplate, double cost) {
    this(mvTableNameWithType, matchType, execMode, mvQuery, baseQueryTemplate, cost, null, 0);
  }

  public MvRewritePlan(String mvTableNameWithType, MatchType matchType,
      @Nullable ExecutionMode execMode, PinotQuery mvQuery,
      @Nullable PinotQuery baseQueryTemplate, double cost,
      @Nullable MvSplitSpec splitSpec, long coverageUpperMs) {
    _mvTableNameWithType = mvTableNameWithType;
    _matchType = matchType;
    _execMode = execMode;
    _mvQuery = mvQuery;
    _baseQueryTemplate = baseQueryTemplate;
    _cost = cost;
    _splitSpec = splitSpec;
    _coverageUpperMs = coverageUpperMs;
  }

  /**
   * Returns a new plan with execution mode, base query template, and split
   * runtime parameters set, preserving all other fields from this plan.
   */
  public MvRewritePlan withExecMode(ExecutionMode execMode, @Nullable PinotQuery baseQueryTemplate,
      @Nullable MvSplitSpec splitSpec, long coverageUpperMs) {
    return new MvRewritePlan(_mvTableNameWithType, _matchType, execMode,
        _mvQuery, baseQueryTemplate, _cost, splitSpec, coverageUpperMs);
  }

  public String getMvTableNameWithType() {
    return _mvTableNameWithType;
  }

  public MatchType getMatchType() {
    return _matchType;
  }

  @Nullable
  public ExecutionMode getExecMode() {
    return _execMode;
  }

  public PinotQuery getMvQuery() {
    return _mvQuery;
  }

  @Nullable
  public PinotQuery getBaseQueryTemplate() {
    return _baseQueryTemplate;
  }

  public double getCost() {
    return _cost;
  }

  @Nullable
  public MvSplitSpec getSplitSpec() {
    return _splitSpec;
  }

  public long getCoverageUpperMs() {
    return _coverageUpperMs;
  }

  @Override
  public int compareTo(MvRewritePlan other) {
    return Double.compare(_cost, other._cost);
  }
}
