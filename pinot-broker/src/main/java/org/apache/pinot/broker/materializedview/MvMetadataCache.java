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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.minion.MvDefinitionMetadata;
import org.apache.pinot.common.minion.MvDefinitionMetadata.MvSplitSpec;
import org.apache.pinot.common.minion.MvFreshness;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Broker-side cache that maintains a reverse index from base table names to their
 * materialized view (MV) entries. Subscribes to two ZK paths:
 * <ul>
 *   <li>{@code /CONFIGS/MV_DEFINITION} — low-frequency changes trigger SQL recompilation</li>
 *   <li>{@code /CONFIGS/MV_RUNTIME} — high-frequency changes only update scalar fields
 *       (coverageUpperMs, freshness) without any SQL parsing</li>
 * </ul>
 *
 * <p>Thread-safety: all mutations go through synchronized ZK listener callbacks;
 * reads use a {@link ConcurrentHashMap} and are lock-free.
 */
public class MvMetadataCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(MvMetadataCache.class);

  private static final String MV_DEFINITION_PARENT_PATH =
      ZKMetadataProvider.getPropertyStorePathForMvDefinitionPrefix();
  private static final String MV_DEFINITION_PATH_PREFIX = MV_DEFINITION_PARENT_PATH + "/";
  private static final String MV_RUNTIME_PARENT_PATH =
      ZKMetadataProvider.getPropertyStorePathForMvRuntimePrefix();
  private static final String MV_RUNTIME_PATH_PREFIX = MV_RUNTIME_PARENT_PATH + "/";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final ZkDefinitionListener _definitionListener = new ZkDefinitionListener();
  private final ZkRuntimeListener _runtimeListener = new ZkRuntimeListener();

  private final Map<String, MvCacheEntry> _mvEntryMap = new ConcurrentHashMap<>();
  private final Map<String, List<MvCacheEntry>> _baseTableToMvMap = new ConcurrentHashMap<>();

  public MvMetadataCache(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;

    synchronized (_definitionListener) {
      _propertyStore.subscribeChildChanges(MV_DEFINITION_PARENT_PATH, _definitionListener);
      _propertyStore.subscribeChildChanges(MV_RUNTIME_PARENT_PATH, _runtimeListener);

      List<String> defChildren =
          _propertyStore.getChildNames(MV_DEFINITION_PARENT_PATH, AccessOption.PERSISTENT);
      if (CollectionUtils.isNotEmpty(defChildren)) {
        List<String> defPaths = new ArrayList<>(defChildren.size());
        List<String> rtPaths = new ArrayList<>(defChildren.size());
        for (String mvTableName : defChildren) {
          defPaths.add(MV_DEFINITION_PATH_PREFIX + mvTableName);
          rtPaths.add(MV_RUNTIME_PATH_PREFIX + mvTableName);
        }
        addDefinitions(defPaths);
        loadRuntimeStates(rtPaths);
      }
    }

    LOGGER.info("Initialized MvMetadataCache with {} MV entries", _mvEntryMap.size());
  }

  @Nullable
  public List<MvCacheEntry> getMvEntriesForBaseTable(String rawBaseTableName) {
    return _baseTableToMvMap.get(rawBaseTableName);
  }

  // -----------------------------------------------------------------------
  //  Definition path handling (low frequency — triggers SQL recompilation)
  // -----------------------------------------------------------------------

  private void addDefinitions(List<String> paths) {
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _definitionListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          putDefinitionEntry(znRecord);
        } catch (Exception e) {
          LOGGER.error("Failed to add MV definition for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private void putDefinitionEntry(ZNRecord znRecord) {
    MvDefinitionMetadata definition = MvDefinitionMetadata.fromZNRecord(znRecord);
    String mvTableNameWithType = definition.getMvTableNameWithType();

    PinotQuery compiledQuery = null;
    String definedSql = definition.getDefinedSql();
    if (definedSql != null && !definedSql.isEmpty()) {
      try {
        compiledQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
      } catch (Exception e) {
        LOGGER.warn("Failed to compile definedSql for MV {}: {}", mvTableNameWithType, definedSql, e);
      }
    }

    MvCacheEntry existing = _mvEntryMap.get(mvTableNameWithType);
    MvCacheEntry newEntry;
    if (existing != null) {
      newEntry = new MvCacheEntry(definition, compiledQuery,
          existing._coverageUpperMs, existing._freshness);
    } else {
      newEntry = new MvCacheEntry(definition, compiledQuery, 0L, MvFreshness.FRESH);
    }

    MvCacheEntry oldEntry = _mvEntryMap.put(mvTableNameWithType, newEntry);
    if (oldEntry != null) {
      removeFromReverseIndex(oldEntry);
    }
    addToReverseIndex(newEntry);
  }

  private void removeDefinitionEntry(String path) {
    _propertyStore.unsubscribeDataChanges(path, _definitionListener);
    String mvTableNameWithType = path.substring(MV_DEFINITION_PATH_PREFIX.length());
    MvCacheEntry removed = _mvEntryMap.remove(mvTableNameWithType);
    if (removed != null) {
      removeFromReverseIndex(removed);
    }
  }

  // -----------------------------------------------------------------------
  //  Runtime path handling (high frequency — only updates scalar fields)
  // -----------------------------------------------------------------------

  private void loadRuntimeStates(List<String> paths) {
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _runtimeListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          updateRuntimeState(znRecord);
        } catch (Exception e) {
          LOGGER.error("Failed to load MV runtime for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private void updateRuntimeState(ZNRecord znRecord) {
    String mvTableNameWithType = znRecord.getId();
    MvCacheEntry entry = _mvEntryMap.get(mvTableNameWithType);
    if (entry == null) {
      LOGGER.debug("Ignoring runtime update for unknown MV: {}", mvTableNameWithType);
      return;
    }

    // New-format ZNodes have the "watermarkMs" key; legacy ZNodes only have
    // "upperExclusiveMs". For legacy ZNodes, fall back to upperExclusiveMs.
    // For new-format ZNodes, missing coverageUpperMs means 0 (cold-start).
    long coverageUpperMs;
    if (znRecord.getSimpleFields().containsKey("watermarkMs")) {
      coverageUpperMs = znRecord.getLongField("coverageUpperMs", 0L);
    } else {
      coverageUpperMs = znRecord.getLongField("upperExclusiveMs", 0L);
    }
    String freshnessStr = znRecord.getSimpleField("freshness");
    MvFreshness freshness = freshnessStr != null
        ? MvFreshness.valueOf(freshnessStr) : MvFreshness.FRESH;

    entry._coverageUpperMs = coverageUpperMs;
    entry._freshness = freshness;
  }

  // -----------------------------------------------------------------------
  //  Reverse index management
  // -----------------------------------------------------------------------

  private void addToReverseIndex(MvCacheEntry entry) {
    for (String baseTable : entry.getDefinition().getBaseTables()) {
      _baseTableToMvMap.compute(baseTable, (key, existing) -> {
        if (existing == null) {
          List<MvCacheEntry> list = new ArrayList<>();
          list.add(entry);
          return list;
        }
        List<MvCacheEntry> updated = new ArrayList<>(existing);
        updated.add(entry);
        return Collections.unmodifiableList(updated);
      });
    }
  }

  private void removeFromReverseIndex(MvCacheEntry entry) {
    for (String baseTable : entry.getDefinition().getBaseTables()) {
      _baseTableToMvMap.compute(baseTable, (key, existing) -> {
        if (existing == null) {
          return null;
        }
        List<MvCacheEntry> updated = new ArrayList<>(existing);
        updated.removeIf(e -> e.getDefinition().getMvTableNameWithType()
            .equals(entry.getDefinition().getMvTableNameWithType()));
        return updated.isEmpty() ? null : Collections.unmodifiableList(updated);
      });
    }
  }

  // -----------------------------------------------------------------------
  //  ZK listeners
  // -----------------------------------------------------------------------

  private class ZkDefinitionListener implements IZkChildListener, IZkDataListener {
    @Override
    public synchronized void handleChildChange(String path, List<String> children) {
      Set<String> newChildSet = CollectionUtils.isNotEmpty(children)
          ? new HashSet<>(children) : Collections.emptySet();

      // Evict entries that are no longer present in ZK (MV table deleted).
      for (String existing : new ArrayList<>(_mvEntryMap.keySet())) {
        if (!newChildSet.contains(existing)) {
          removeDefinitionEntry(MV_DEFINITION_PATH_PREFIX + existing);
        }
      }

      // Register newly added entries.
      List<String> defPathsToAdd = new ArrayList<>();
      List<String> rtPathsToAdd = new ArrayList<>();
      for (String mvTableName : newChildSet) {
        if (!_mvEntryMap.containsKey(mvTableName)) {
          defPathsToAdd.add(MV_DEFINITION_PATH_PREFIX + mvTableName);
          rtPathsToAdd.add(MV_RUNTIME_PATH_PREFIX + mvTableName);
        }
      }
      if (!defPathsToAdd.isEmpty()) {
        addDefinitions(defPathsToAdd);
        loadRuntimeStates(rtPathsToAdd);
      }
    }

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        try {
          putDefinitionEntry((ZNRecord) data);
        } catch (Exception e) {
          LOGGER.error("Failed to refresh MV definition for: {}", path, e);
        }
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      removeDefinitionEntry(path);
    }
  }

  private class ZkRuntimeListener implements IZkChildListener, IZkDataListener {
    @Override
    public synchronized void handleChildChange(String path, List<String> children) {
      Set<String> newChildSet = CollectionUtils.isNotEmpty(children)
          ? new HashSet<>(children) : Collections.emptySet();

      // Unsubscribe data listeners for runtime nodes that are no longer present.
      for (String mvTableName : _mvEntryMap.keySet()) {
        if (!newChildSet.contains(mvTableName)) {
          _propertyStore.unsubscribeDataChanges(MV_RUNTIME_PATH_PREFIX + mvTableName, _runtimeListener);
        }
      }

      // Subscribe for any newly visible runtime nodes.
      List<String> rtPathsToAdd = new ArrayList<>();
      for (String mvTableName : newChildSet) {
        if (_mvEntryMap.containsKey(mvTableName)) {
          rtPathsToAdd.add(MV_RUNTIME_PATH_PREFIX + mvTableName);
        }
      }
      if (!rtPathsToAdd.isEmpty()) {
        loadRuntimeStates(rtPathsToAdd);
      }
    }

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        try {
          updateRuntimeState((ZNRecord) data);
        } catch (Exception e) {
          LOGGER.error("Failed to refresh MV runtime for: {}", path, e);
        }
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      // Runtime deletion is handled when the definition is deleted;
      // just unsubscribe.
      _propertyStore.unsubscribeDataChanges(path, _runtimeListener);
    }
  }

  // -----------------------------------------------------------------------
  //  Cache entry
  // -----------------------------------------------------------------------

  /**
   * A cached entry holding the MV definition (with pre-compiled {@link PinotQuery})
   * and volatile runtime scalars (coverageUpperMs, freshness).
   *
   * <p>The broker only needs {@code coverageUpperMs} (confirmed queryable data
   * boundary) and not the generator's scheduling watermark.
   */
  public static class MvCacheEntry {
    private final MvDefinitionMetadata _definition;
    private final PinotQuery _compiledQuery;

    volatile long _coverageUpperMs;
    volatile MvFreshness _freshness;

    public MvCacheEntry(MvDefinitionMetadata definition, @Nullable PinotQuery compiledQuery,
        long coverageUpperMs, MvFreshness freshness) {
      _definition = definition;
      _compiledQuery = compiledQuery;
      _coverageUpperMs = coverageUpperMs;
      _freshness = freshness;
    }

    public MvDefinitionMetadata getDefinition() {
      return _definition;
    }

    @Nullable
    public PinotQuery getCompiledQuery() {
      return _compiledQuery;
    }

    public long getCoverageUpperMs() {
      return _coverageUpperMs;
    }

    public MvFreshness getFreshness() {
      return _freshness;
    }

    @Nullable
    public MvSplitSpec getSplitSpec() {
      return _definition.getSplitSpec();
    }

    public String getMvTableNameWithType() {
      return _definition.getMvTableNameWithType();
    }
  }
}
