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

import org.apache.pinot.common.minion.MaterializedViewMetadata;
import org.apache.pinot.common.request.PinotQuery;


/**
 * Holds the result of a successful MV match: the rewritten query targeting the MV table,
 * the matched metadata, and a cost score for ranking among multiple candidates.
 *
 * <p>Lower cost is better. The rewrite engine picks the result with the lowest cost
 * when multiple MVs match a single user query.
 */
public class MvMatchResult implements Comparable<MvMatchResult> {
  private final PinotQuery _rewrittenQuery;
  private final MaterializedViewMetadata _metadata;
  private final double _cost;

  public MvMatchResult(PinotQuery rewrittenQuery, MaterializedViewMetadata metadata, double cost) {
    _rewrittenQuery = rewrittenQuery;
    _metadata = metadata;
    _cost = cost;
  }

  public PinotQuery getRewrittenQuery() {
    return _rewrittenQuery;
  }

  public MaterializedViewMetadata getMetadata() {
    return _metadata;
  }

  public String getMvTableName() {
    return _metadata.getMvTableNameWithType();
  }

  public double getCost() {
    return _cost;
  }

  @Override
  public int compareTo(MvMatchResult other) {
    return Double.compare(_cost, other._cost);
  }
}
