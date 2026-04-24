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
package org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.rule;

import com.google.common.base.Preconditions;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.FormatMatcher;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.InferredTimeSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec;


/**
 * "No transformation" case: the MV time column SELECT expression is a bare identifier
 * referring to the base table's primary time column.
 *
 * <p>This is handled by the inferrer dispatch directly (not via
 * {@link org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeFunctionRule}),
 * so this class exposes {@link #inferIdentity} rather than implementing the normal rule
 * contract. It is kept in the {@code rule} package for locality with other rules and ease
 * of future refactoring (e.g. if the inferrer is rewritten to look up a rule uniformly).
 */
public final class IdentityRule {

  public static final IdentityRule INSTANCE = new IdentityRule();

  private IdentityRule() {
  }

  /**
   * Expected output: the base column's own format and granularity, verbatim.
   */
  public InferredTimeSpec inferIdentity(Expression sourceExpr, String baseTimeColumnName,
      DateTimeFieldSpec baseTimeFieldSpec) {
    Preconditions.checkState(sourceExpr.getType() == ExpressionType.IDENTIFIER,
        "IdentityRule invoked for non-identifier expression: %s", sourceExpr);
    String actual = sourceExpr.getIdentifier().getName();
    Preconditions.checkState(baseTimeColumnName.equals(actual),
        "MV time column must derive from base time column '%s' via identity or a whitelisted "
            + "function, got identifier '%s'.",
        baseTimeColumnName, actual);
    return new InferredTimeSpec(
        FormatMatcher.exact(baseTimeFieldSpec.getFormat()),
        baseTimeFieldSpec.getGranularity(),
        "identity: MV time column is the base time column '" + baseTimeColumnName
            + "' unchanged, so format and granularity must match base exactly");
  }
}
