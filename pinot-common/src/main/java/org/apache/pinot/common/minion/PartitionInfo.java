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
package org.apache.pinot.common.minion;

import java.util.Objects;


/**
 * Tracks the state and provenance of a single materialized partition.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@code state} – whether the partition is up-to-date ({@link PartitionState#VALID})
 *       or needs re-materialization ({@link PartitionState#STALE}).</li>
 *   <li>{@code fingerprint} – the base segment snapshot (count + CRC) recorded when
 *       the partition was last materialized.</li>
 *   <li>{@code lastRefreshMs} – wall clock time (millis) of the last successful materialization.</li>
 * </ul>
 *
 * <p>Serialized as {@code "V,10,5000,1700006400000"} (state, segCount, crc, lastRefreshMs)
 * for compact ZNRecord map field storage.
 *
 * <p>Thread-safety: instances are immutable after construction.
 */
public class PartitionInfo {
  private static final char SEP = ',';

  private final PartitionState _state;
  private final PartitionFingerprint _fingerprint;
  private final long _lastRefreshMs;

  public PartitionInfo(PartitionState state, PartitionFingerprint fingerprint, long lastRefreshMs) {
    _state = state;
    _fingerprint = fingerprint;
    _lastRefreshMs = lastRefreshMs;
  }

  public PartitionState getState() {
    return _state;
  }

  public PartitionFingerprint getFingerprint() {
    return _fingerprint;
  }

  public long getLastRefreshMs() {
    return _lastRefreshMs;
  }

  /**
   * Creates a new {@code PartitionInfo} with the given state, keeping fingerprint and
   * lastRefreshMs unchanged.
   */
  public PartitionInfo withState(PartitionState newState) {
    return new PartitionInfo(newState, _fingerprint, _lastRefreshMs);
  }

  /**
   * Encodes as {@code "state,segCount,crc,lastRefreshMs"}.
   */
  public String encode() {
    return _state.encode() + SEP + _fingerprint.getSegmentCount() + SEP
        + _fingerprint.getCrcChecksum() + SEP + _lastRefreshMs;
  }

  /**
   * Decodes from the format {@code "state,segCount,crc,lastRefreshMs"}.
   *
   * @throws IllegalArgumentException if the string is malformed
   */
  public static PartitionInfo decode(String encoded) {
    int first = encoded.indexOf(SEP);
    if (first < 0) {
      throw new IllegalArgumentException("Invalid PartitionInfo encoding: " + encoded);
    }
    PartitionState state = PartitionState.decode(encoded.substring(0, first));

    int second = encoded.indexOf(SEP, first + 1);
    if (second < 0) {
      throw new IllegalArgumentException("Invalid PartitionInfo encoding: " + encoded);
    }
    int segmentCount = Integer.parseInt(encoded.substring(first + 1, second));

    int third = encoded.indexOf(SEP, second + 1);
    if (third < 0) {
      throw new IllegalArgumentException("Invalid PartitionInfo encoding: " + encoded);
    }
    long crcChecksum = Long.parseLong(encoded.substring(second + 1, third));
    long lastRefreshMs = Long.parseLong(encoded.substring(third + 1));

    return new PartitionInfo(state, new PartitionFingerprint(segmentCount, crcChecksum), lastRefreshMs);
  }

  /**
   * Creates a {@code PartitionInfo} from a legacy {@link PartitionFingerprint} (no state or
   * refresh time was tracked). Defaults to {@code VALID} with {@code lastRefreshMs = 0}.
   */
  public static PartitionInfo fromLegacyFingerprint(PartitionFingerprint fingerprint) {
    return new PartitionInfo(PartitionState.VALID, fingerprint, 0L);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionInfo that = (PartitionInfo) o;
    return _lastRefreshMs == that._lastRefreshMs
        && _state == that._state
        && Objects.equals(_fingerprint, that._fingerprint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_state, _fingerprint, _lastRefreshMs);
  }

  @Override
  public String toString() {
    return "PartitionInfo{state=" + _state + ", fingerprint=" + _fingerprint
        + ", lastRefreshMs=" + _lastRefreshMs + "}";
  }
}
