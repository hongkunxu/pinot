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
 * State of a materialized partition.
 *
 * <ul>
 *   <li>{@code VALID} – partition is up-to-date with base table data.</li>
 *   <li>{@code STALE} – base table data has changed since last materialization;
 *       partition needs to be overwritten.</li>
 * </ul>
 *
 * <p>Encoded as a single character ({@code "V"} / {@code "S"}) for compact ZK storage.
 */
public enum PartitionState {
  VALID("V"),
  STALE("S");

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
      default:
        throw new IllegalArgumentException("Unknown PartitionState code: " + code);
    }
  }
}
