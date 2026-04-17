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

import java.util.List;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.broker.materializedview.strategy.AggregationSubsumptionStrategy;
import org.apache.pinot.broker.materializedview.strategy.ExactSubsumptionStrategy;
import org.apache.pinot.broker.materializedview.strategy.ScanSubsumptionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Factory for the broker's default {@link MvQueryRewriteEngine} wiring.
 *
 * <p>Default strategy order (highest precision first, per {@link MvQueryRewriteEngine}):
 * {@link ExactSubsumptionStrategy}, {@link ScanSubsumptionStrategy},
 * {@link AggregationSubsumptionStrategy}.
 *
 * <p>Thread-safety: the returned engine is immutable and safe to share across components.
 */
public final class MvQueryRewriteEngineFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(MvQueryRewriteEngineFactory.class);

  private MvQueryRewriteEngineFactory() {
  }

  /**
   * Builds a {@link MvQueryRewriteEngine} backed by a new {@link MvMetadataCache} on the given
   * Helix property store.
   */
  public static MvQueryRewriteEngine createDefault(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    LOGGER.info("Initializing MV metadata cache and query rewrite engine");
    MvMetadataCache mvMetadataCache = new MvMetadataCache(propertyStore);
    return new MvQueryRewriteEngine(mvMetadataCache, List.of(
        new ExactSubsumptionStrategy(),
        new ScanSubsumptionStrategy(),
        new AggregationSubsumptionStrategy()));
  }
}
