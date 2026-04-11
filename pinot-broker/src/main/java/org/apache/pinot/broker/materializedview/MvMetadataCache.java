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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.pinot.common.minion.MaterializedViewMetadata;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Broker-side cache that maintains a reverse index from base table names to their
 * materialized view (MV) metadata. Listens on ZK path {@code /CONFIGS/MATERIALIZED_VIEW}
 * for real-time updates using the same child+data listener pattern as {@code ZkTableCache}.
 *
 * <p>For each MV, the {@code definedSql} is pre-compiled into a {@link PinotQuery} to
 * avoid repeated parsing on every user query.
 *
 * <p>Thread-safety: all mutations go through synchronized ZK listener callbacks;
 * reads use a {@link ConcurrentHashMap} and are lock-free.
 */
public class MvMetadataCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(MvMetadataCache.class);

  private static final String MV_METADATA_PARENT_PATH = "/CONFIGS/MATERIALIZED_VIEW";
  private static final String MV_METADATA_PATH_PREFIX = "/CONFIGS/MATERIALIZED_VIEW/";

  private final ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private final ZkMvMetadataChangeListener _zkListener = new ZkMvMetadataChangeListener();

  // mvTableNameWithType -> cached entry (metadata + pre-compiled PinotQuery)
  private final Map<String, MvCacheEntry> _mvEntryMap = new ConcurrentHashMap<>();

  // baseTable (raw name) -> list of MvCacheEntry for that base table
  private final Map<String, List<MvCacheEntry>> _baseTableToMvMap = new ConcurrentHashMap<>();

  public MvMetadataCache(ZkHelixPropertyStore<ZNRecord> propertyStore) {
    _propertyStore = propertyStore;

    synchronized (_zkListener) {
      _propertyStore.subscribeChildChanges(MV_METADATA_PARENT_PATH, _zkListener);

      List<String> children = _propertyStore.getChildNames(MV_METADATA_PARENT_PATH, AccessOption.PERSISTENT);
      if (CollectionUtils.isNotEmpty(children)) {
        List<String> pathsToAdd = new ArrayList<>(children.size());
        for (String mvTableName : children) {
          pathsToAdd.add(MV_METADATA_PATH_PREFIX + mvTableName);
        }
        addMvMetadata(pathsToAdd);
      }
    }

    LOGGER.info("Initialized MvMetadataCache with {} MV entries", _mvEntryMap.size());
  }

  /**
   * Returns all cached MV entries for the given base table, or {@code null} if none exist.
   */
  @Nullable
  public List<MvCacheEntry> getMvEntriesForBaseTable(String rawBaseTableName) {
    return _baseTableToMvMap.get(rawBaseTableName);
  }

  private void addMvMetadata(List<String> paths) {
    for (String path : paths) {
      _propertyStore.subscribeDataChanges(path, _zkListener);
    }
    List<ZNRecord> znRecords = _propertyStore.get(paths, null, AccessOption.PERSISTENT);
    for (ZNRecord znRecord : znRecords) {
      if (znRecord != null) {
        try {
          putMvEntry(znRecord);
        } catch (Exception e) {
          LOGGER.error("Failed to add MV metadata for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }
  }

  private void putMvEntry(ZNRecord znRecord) {
    MaterializedViewMetadata metadata = MaterializedViewMetadata.fromZNRecord(znRecord);
    String mvTableNameWithType = metadata.getMvTableNameWithType();

    PinotQuery compiledQuery = null;
    String definedSql = metadata.getDefinedSql();
    if (definedSql != null && !definedSql.isEmpty()) {
      try {
        compiledQuery = CalciteSqlParser.compileToPinotQuery(definedSql);
      } catch (Exception e) {
        LOGGER.warn("Failed to compile definedSql for MV {}: {}", mvTableNameWithType, definedSql, e);
      }
    }

    MvCacheEntry newEntry = new MvCacheEntry(metadata, compiledQuery);

    MvCacheEntry oldEntry = _mvEntryMap.put(mvTableNameWithType, newEntry);
    if (oldEntry != null) {
      removeFromReverseIndex(oldEntry);
    }
    addToReverseIndex(newEntry);
  }

  private void removeMvEntry(String path) {
    _propertyStore.unsubscribeDataChanges(path, _zkListener);
    String mvTableNameWithType = path.substring(MV_METADATA_PATH_PREFIX.length());
    MvCacheEntry removed = _mvEntryMap.remove(mvTableNameWithType);
    if (removed != null) {
      removeFromReverseIndex(removed);
    }
  }

  private void addToReverseIndex(MvCacheEntry entry) {
    for (String baseTable : entry.getMetadata().getBaseTables()) {
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
    for (String baseTable : entry.getMetadata().getBaseTables()) {
      _baseTableToMvMap.compute(baseTable, (key, existing) -> {
        if (existing == null) {
          return null;
        }
        List<MvCacheEntry> updated = new ArrayList<>(existing);
        updated.removeIf(e -> e.getMetadata().getMvTableNameWithType()
            .equals(entry.getMetadata().getMvTableNameWithType()));
        return updated.isEmpty() ? null : Collections.unmodifiableList(updated);
      });
    }
  }

  private class ZkMvMetadataChangeListener implements IZkChildListener, IZkDataListener {

    @Override
    public synchronized void handleChildChange(String path, List<String> children) {
      if (CollectionUtils.isEmpty(children)) {
        return;
      }
      List<String> pathsToAdd = new ArrayList<>();
      for (String mvTableName : children) {
        if (!_mvEntryMap.containsKey(mvTableName)) {
          pathsToAdd.add(MV_METADATA_PATH_PREFIX + mvTableName);
        }
      }
      if (!pathsToAdd.isEmpty()) {
        addMvMetadata(pathsToAdd);
      }
    }

    @Override
    public synchronized void handleDataChange(String path, Object data) {
      if (data != null) {
        ZNRecord znRecord = (ZNRecord) data;
        try {
          putMvEntry(znRecord);
        } catch (Exception e) {
          LOGGER.error("Failed to refresh MV metadata for ZNRecord: {}", znRecord.getId(), e);
        }
      }
    }

    @Override
    public synchronized void handleDataDeleted(String path) {
      String mvTableNameWithType = path.substring(path.lastIndexOf('/') + 1);
      removeMvEntry(MV_METADATA_PATH_PREFIX + mvTableNameWithType);
    }
  }

  /**
   * A cached entry holding both the raw {@link MaterializedViewMetadata} and the
   * pre-compiled {@link PinotQuery} from its {@code definedSql}.
   */
  public static class MvCacheEntry {
    private final MaterializedViewMetadata _metadata;
    private final PinotQuery _compiledQuery;

    public MvCacheEntry(MaterializedViewMetadata metadata, @Nullable PinotQuery compiledQuery) {
      _metadata = metadata;
      _compiledQuery = compiledQuery;
    }

    public MaterializedViewMetadata getMetadata() {
      return _metadata;
    }

    @Nullable
    public PinotQuery getCompiledQuery() {
      return _compiledQuery;
    }
  }
}
