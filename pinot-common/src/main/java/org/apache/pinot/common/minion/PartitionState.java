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


/**
 * State of a materialized partition in the MV lifecycle.
 *
 * <ul>
 *   <li>{@code VALID} – partition is up-to-date with base table data.</li>
 *   <li>{@code STALE} – base table data has changed (segments updated/replaced)
 *       since last materialization; partition needs OVERWRITE.</li>
 *   <li>{@code EXPIRED} – base table data no longer exists (segments deleted by
 *       retention or manually); partition needs DELETE.</li>
 * </ul>
 *
 * <p>Encoded as a single character ({@code "V"} / {@code "S"} / {@code "E"})
 * for compact ZK storage.
 *
 * <h3>State machine</h3>
 * <pre>
 *                        APPEND success
 *   [not in map] ──────────────────────────&gt; VALID
 *                                              |
 *                                +-------------+-------------+
 *                                |                           |
 *                      base data changed           base data deleted
 *                     "fingerprint mismatch,      "fingerprint mismatch,
 *                       segmentCount &gt; 0"           segmentCount == 0"
 *                                |                           |
 *                                v                           v
 *                              STALE                      EXPIRED
 *                                |                           |
 *                      +---------+---------+                 |
 *                      |                   |                 |
 *             OVERWRITE success    base data deleted    DELETE success
 *                      |                   |                 |
 *                      v                   v                 v
 *                    VALID              EXPIRED         [removed from map]
 *
 *   Failure paths (automatic retry on next scheduling cycle):
 *     STALE   --(OVERWRITE fails)--&gt; STALE
 *     EXPIRED --(DELETE fails)-----&gt; EXPIRED
 * </pre>
 */
public enum PartitionState {
  VALID("V"),
  STALE("S"),
  EXPIRED("E");

  private final String _code;

  PartitionState(String code) {
    _code = code;
  }

  public String encode() {
    return _code;
  }

  public static PartitionState decode(String code) {
    switch (code) {
      case "V":
        return VALID;
      case "S":
        return STALE;
      case "E":
        return EXPIRED;
      default:
        throw new IllegalArgumentException("Unknown PartitionState code: " + code);
    }
  }
}
