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
package org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.rule.DateTimeConvertRule;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.rule.DateTruncRule;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.rule.ToDateTimeRule;


/**
 * Static whitelist of time-conversion functions permitted in the MV time-column SELECT
 * expression. Adding a new function means:
 * <ol>
 *   <li>Implement a new {@link TimeFunctionRule}.</li>
 *   <li>Register it here under its canonical name.</li>
 *   <li>Add per-rule unit tests plus one analyzer-level end-to-end test.</li>
 * </ol>
 * The analyzer's main path does not change.
 *
 * <p>Keys must already be canonicalized (see
 * {@link org.apache.pinot.common.function.FunctionRegistry#canonicalize}).
 */
public final class TimeFunctionRegistry {

  private static final Map<String, TimeFunctionRule> RULES;

  static {
    Map<String, TimeFunctionRule> rules = new LinkedHashMap<>();
    register(rules, new DateTimeConvertRule());
    register(rules, new DateTruncRule());
    register(rules, new ToDateTimeRule());
    RULES = Collections.unmodifiableMap(rules);
  }

  private TimeFunctionRegistry() {
  }

  private static void register(Map<String, TimeFunctionRule> rules, TimeFunctionRule rule) {
    TimeFunctionRule prev = rules.put(rule.canonicalName(), rule);
    if (prev != null) {
      throw new IllegalStateException(
          "Duplicate TimeFunctionRule registration for canonical name '" + rule.canonicalName() + "'");
    }
  }

  /** Returns the rule registered for {@code canonicalName}, or {@code null} if unsupported. */
  @Nullable
  public static TimeFunctionRule get(String canonicalName) {
    return RULES.get(canonicalName);
  }

  /** Returns the full set of supported canonical function names (for error messages). */
  public static Set<String> supportedCanonicalNames() {
    return RULES.keySet();
  }
}
