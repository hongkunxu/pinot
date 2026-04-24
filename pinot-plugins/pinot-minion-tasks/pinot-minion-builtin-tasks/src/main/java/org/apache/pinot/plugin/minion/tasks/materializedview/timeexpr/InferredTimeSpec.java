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

import javax.annotation.Nullable;


/**
 * Immutable result of time-expression inference.
 *
 * <p>Carries a {@link FormatMatcher} (never {@code null}) and an optional expected
 * granularity string. A {@code null} granularity means the rule deliberately chose not to
 * infer it — in v1 this is only the case for {@code toDateTime}, where the function itself
 * carries no bucket-granularity semantics. The caller treats a {@code null} granularity as
 * "skip the strict-equality check, but still require the MV fieldSpec to declare a
 * granularity".
 *
 * <p>Also carries a short human-readable {@link #reason()} that the analyzer surfaces
 * verbatim in mismatch error messages. This is how rules explain non-obvious derivations
 * (e.g. {@code date_trunc}'s output format reflecting storage unit rather than bucket
 * granularity) at the point the user sees the error.
 */
public final class InferredTimeSpec {
  private final FormatMatcher _formatMatcher;
  @Nullable
  private final String _expectedGranularity;
  private final String _reason;

  public InferredTimeSpec(FormatMatcher formatMatcher, @Nullable String expectedGranularity, String reason) {
    _formatMatcher = formatMatcher;
    _expectedGranularity = expectedGranularity;
    _reason = reason;
  }

  public FormatMatcher formatMatcher() {
    return _formatMatcher;
  }

  @Nullable
  public String expectedGranularity() {
    return _expectedGranularity;
  }

  public String reason() {
    return _reason;
  }
}
