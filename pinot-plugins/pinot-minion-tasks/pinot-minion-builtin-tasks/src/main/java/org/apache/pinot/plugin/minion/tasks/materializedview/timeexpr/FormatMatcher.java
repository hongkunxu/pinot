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

/**
 * Strategy for matching a rule's inferred output format against an MV
 * {@link org.apache.pinot.spi.data.DateTimeFieldSpec#getFormat()} string.
 *
 * <p>Two implementations are provided:
 * <ul>
 *   <li>{@link #exact(String)} — full-string strict equality. Used by identity,
 *       {@code dateTimeConvert} and {@code date_trunc}, which yield a deterministic format
 *       string.</li>
 *   <li>{@link #sdfPattern(String)} — partial match for {@code toDateTime}: the MV format
 *       must be {@code "<size>:<unit>:SIMPLE_DATE_FORMAT:<pattern>"} where {@code <pattern>}
 *       equals the function's pattern argument verbatim. The {@code size:unit} prefix is
 *       not inferred because {@code toDateTime} does not carry a bucket semantic. This is
 *       an explicit, documented deviation from the "strict equality" policy.</li>
 * </ul>
 */
public interface FormatMatcher {

  /** Returns {@code true} iff {@code mvFormat} satisfies the expectation. */
  boolean matches(String mvFormat);

  /** Human-readable description of the expected format for error messages. */
  String describeExpected();

  static FormatMatcher exact(String expected) {
    return new ExactFormatMatcher(expected);
  }

  static FormatMatcher sdfPattern(String expectedPattern) {
    return new SdfPatternFormatMatcher(expectedPattern);
  }

  final class ExactFormatMatcher implements FormatMatcher {
    private final String _expected;

    ExactFormatMatcher(String expected) {
      _expected = expected;
    }

    @Override
    public boolean matches(String mvFormat) {
      return _expected.equals(mvFormat);
    }

    @Override
    public String describeExpected() {
      return _expected;
    }
  }

  /**
   * Matches an MV format string of the form {@code "<size>:<unit>:SIMPLE_DATE_FORMAT:<pattern>"}
   * where {@code <pattern>} is the remainder of the string after the third colon and must
   * strictly equal the expected pattern. The {@code <pattern>} itself may contain colons
   * (e.g. {@code yyyy-MM-dd HH:mm:ss}) — we preserve them by splitting at most 4 times.
   */
  final class SdfPatternFormatMatcher implements FormatMatcher {
    private static final String SDF_MARKER = "SIMPLE_DATE_FORMAT";
    private final String _expectedPattern;

    SdfPatternFormatMatcher(String expectedPattern) {
      _expectedPattern = expectedPattern;
    }

    @Override
    public boolean matches(String mvFormat) {
      if (mvFormat == null) {
        return false;
      }
      // Expect the first three ':'-separated tokens to be <size>, <unit>, SIMPLE_DATE_FORMAT,
      // with everything after the third colon being the pattern (which may contain colons).
      String[] parts = mvFormat.split(":", 4);
      if (parts.length != 4) {
        return false;
      }
      if (!SDF_MARKER.equalsIgnoreCase(parts[2])) {
        return false;
      }
      return _expectedPattern.equals(parts[3]);
    }

    @Override
    public String describeExpected() {
      return "<size>:<unit>:SIMPLE_DATE_FORMAT:" + _expectedPattern;
    }
  }
}
