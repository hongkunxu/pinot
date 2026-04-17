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
package org.apache.pinot.broker.materializedview.equivalence;

import java.util.List;
import javax.annotation.Nullable;


/**
 * Centralized registry of {@link AggregationEquivalence} rules for
 * re-aggregating pre-computed MV columns when the MV has finer granularity
 * than the user query.
 *
 * <p>All rules are statically initialized at class-load time. No external
 * registration or lifecycle management is required.
 *
 * <p>To add support for a new aggregation function equivalence, simply add
 * the corresponding rule instance to the {@link #RULES} list.
 *
 * <p>This class is stateless and thread-safe.
 */
public final class AggregationEquivalenceRegistry {

  private static final List<AggregationEquivalence> RULES = List.of(
      // Distributive: re-aggregate with the same function
      new PassthroughEquivalence("SUM", "SUM"),
      new PassthroughEquivalence("MIN", "MIN"),
      new PassthroughEquivalence("MAX", "MAX"),

      // Algebraic (simple transformation): COUNT -> SUM
      new PassthroughEquivalence("COUNT", "SUM"),

      // Sketch-based: user wants cardinality/result, MV stores raw sketch
      new SketchMergeEquivalence("DISTINCTCOUNTHLL", "DISTINCTCOUNTRAWHLL", "DISTINCTCOUNTHLL"),
      new SketchMergeEquivalence("DISTINCTCOUNTHLLPLUS", "DISTINCTCOUNTRAWHLLPLUS", "DISTINCTCOUNTHLLPLUS"),
      new SketchMergeEquivalence("DISTINCTCOUNTTHETASKETCH", "DISTINCTCOUNTRAWTHETASKETCH",
          "DISTINCTCOUNTTHETASKETCH")
  );

  private AggregationEquivalenceRegistry() {
  }

  /**
   * Finds the first equivalence rule that can handle the given user-function
   * and MV-function pair.
   *
   * @param userFunctionName the aggregation function name from the user query
   *                         (e.g. "SUM", "DISTINCTCOUNTHLL")
   * @param mvFunctionName   the aggregation function name from the MV
   *                         definition (e.g. "SUM", "DISTINCTCOUNTRAWHLL")
   * @return the matching rule, or {@code null} if no rule supports the
   *         given function pair (indicating the function cannot be
   *         re-aggregated)
   */
  @Nullable
  public static AggregationEquivalence findRule(String userFunctionName, String mvFunctionName) {
    for (AggregationEquivalence rule : RULES) {
      if (rule.matches(userFunctionName, mvFunctionName)) {
        return rule;
      }
    }
    return null;
  }
}
