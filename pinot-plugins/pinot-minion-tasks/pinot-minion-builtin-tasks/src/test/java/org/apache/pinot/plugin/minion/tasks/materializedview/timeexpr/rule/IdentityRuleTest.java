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

import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.InferredTimeSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.BASE_COL;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.identifier;
import static org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprTestSupport.millisBaseSpec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class IdentityRuleTest {

  @Test
  public void happyPathReturnsBaseFormatAndGranularity() {
    DateTimeFieldSpec base = millisBaseSpec();
    InferredTimeSpec spec = IdentityRule.INSTANCE.inferIdentity(identifier(BASE_COL), BASE_COL, base);

    assertTrue(spec.formatMatcher().matches("1:MILLISECONDS:EPOCH"));
    assertEquals(spec.expectedGranularity(), "1:MILLISECONDS");
    assertEquals(spec.formatMatcher().describeExpected(), "1:MILLISECONDS:EPOCH");
  }

  @Test
  public void happyPathPreservesBaseGranularityVerbatim() {
    DateTimeFieldSpec base = new DateTimeFieldSpec(BASE_COL,
        org.apache.pinot.spi.data.FieldSpec.DataType.LONG, "1:HOURS:EPOCH", "1:HOURS");
    InferredTimeSpec spec = IdentityRule.INSTANCE.inferIdentity(identifier(BASE_COL), BASE_COL, base);

    assertEquals(spec.expectedGranularity(), "1:HOURS");
    assertTrue(spec.formatMatcher().matches("1:HOURS:EPOCH"));
  }

  @Test
  public void rejectsIdentifierThatIsNotBaseTimeColumn() {
    DateTimeFieldSpec base = millisBaseSpec();
    Throwable t = expectThrows(IllegalStateException.class,
        () -> IdentityRule.INSTANCE.inferIdentity(identifier("eventTime"), BASE_COL, base));
    assertTrue(t.getMessage().contains("identity"), t.getMessage());
    assertTrue(t.getMessage().contains(BASE_COL), t.getMessage());
    assertTrue(t.getMessage().contains("eventTime"), t.getMessage());
  }
}
