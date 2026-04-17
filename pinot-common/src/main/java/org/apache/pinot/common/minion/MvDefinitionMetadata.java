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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * Stores the static definition of a materialized view (MV): what it is derived from,
 * the SQL that produces it, how time columns map, and what split parameters are needed.
 *
 * <p>Persisted in ZooKeeper under {@code /CONFIGS/MV_DEFINITION/<mvTableNameWithType>}.
 * This ZNode changes only when the MV is created or its definition is altered — never
 * during routine task execution or partition-state changes.
 *
 * <p>Thread-safety: instances are effectively immutable after construction.
 */
public class MvDefinitionMetadata {

  private static final String BASE_TABLES_KEY = "baseTables";
  private static final String DEFINED_SQL_KEY = "definedSql";
  private static final String PARTITION_EXPR_MAPS_KEY = "partitionExprMaps";
  private static final String SPLIT_SOURCE_TIME_COLUMN_KEY = "splitSourceTimeColumn";
  private static final String SPLIT_SOURCE_TIME_FORMAT_KEY = "splitSourceTimeFormat";
  private static final String SPLIT_BUCKET_MS_KEY = "splitBucketMs";

  private static final TypeReference<List<String>> STRING_LIST_TYPE =
      new TypeReference<List<String>>() { };
  private static final TypeReference<Map<String, String>> STRING_MAP_TYPE =
      new TypeReference<Map<String, String>>() { };

  private final String _mvTableNameWithType;
  private final List<String> _baseTables;
  private final String _definedSql;

  /// Maps base-table expression strings to MV column identifiers, recording how each base
  /// table time column expression is transformed into the corresponding MV time column.
  /// For example: {@code {"dateTimeConvert(ts,'1:MILLISECONDS:EPOCH','1:DAYS:EPOCH','1:DAYS')": "mvDay"}}
  /// or for a simple pass-through: {@code {"ts": "ts"}}.
  private final Map<String, String> _partitionExprMaps;

  @Nullable
  private final MvSplitSpec _splitSpec;

  public MvDefinitionMetadata(String mvTableNameWithType, List<String> baseTables,
      String definedSql, Map<String, String> partitionExprMaps,
      @Nullable MvSplitSpec splitSpec) {
    _mvTableNameWithType = mvTableNameWithType;
    _baseTables = baseTables;
    _definedSql = definedSql;
    _partitionExprMaps = partitionExprMaps;
    _splitSpec = splitSpec;
  }

  public String getMvTableNameWithType() {
    return _mvTableNameWithType;
  }

  public List<String> getBaseTables() {
    return _baseTables;
  }

  public String getDefinedSql() {
    return _definedSql;
  }

  public Map<String, String> getPartitionExprMaps() {
    return _partitionExprMaps;
  }

  @Nullable
  public MvSplitSpec getSplitSpec() {
    return _splitSpec;
  }

  public ZNRecord toZNRecord() {
    ZNRecord znRecord = new ZNRecord(_mvTableNameWithType);
    try {
      znRecord.setSimpleField(BASE_TABLES_KEY, JsonUtils.objectToString(_baseTables));
      if (_definedSql != null) {
        znRecord.setSimpleField(DEFINED_SQL_KEY, _definedSql);
      }
      if (_partitionExprMaps != null && !_partitionExprMaps.isEmpty()) {
        znRecord.setSimpleField(PARTITION_EXPR_MAPS_KEY, JsonUtils.objectToString(_partitionExprMaps));
      }
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize MvDefinitionMetadata", e);
    }

    if (_splitSpec != null) {
      znRecord.setSimpleField(SPLIT_SOURCE_TIME_COLUMN_KEY, _splitSpec.getSourceTimeColumn());
      znRecord.setSimpleField(SPLIT_SOURCE_TIME_FORMAT_KEY, _splitSpec.getSourceTimeFormat());
      znRecord.setLongField(SPLIT_BUCKET_MS_KEY, _splitSpec.getBucketMs());
    }

    return znRecord;
  }

  public static MvDefinitionMetadata fromZNRecord(ZNRecord znRecord) {
    String mvTableNameWithType = znRecord.getId();
    try {
      String baseTablesJson = znRecord.getSimpleField(BASE_TABLES_KEY);
      List<String> baseTables = baseTablesJson != null
          ? JsonUtils.stringToObject(baseTablesJson, STRING_LIST_TYPE)
          : Collections.emptyList();

      String definedSql = znRecord.getSimpleField(DEFINED_SQL_KEY);

      String partitionExprMapsJson = znRecord.getSimpleField(PARTITION_EXPR_MAPS_KEY);
      Map<String, String> partitionExprMaps = partitionExprMapsJson != null
          ? JsonUtils.stringToObject(partitionExprMapsJson, STRING_MAP_TYPE)
          : new HashMap<>();

      MvSplitSpec splitSpec = null;
      String sourceTimeColumn = znRecord.getSimpleField(SPLIT_SOURCE_TIME_COLUMN_KEY);
      if (sourceTimeColumn != null) {
        String sourceTimeFormat = znRecord.getSimpleField(SPLIT_SOURCE_TIME_FORMAT_KEY);
        long bucketMs = znRecord.getLongField(SPLIT_BUCKET_MS_KEY, 0L);
        splitSpec = new MvSplitSpec(sourceTimeColumn, sourceTimeFormat, bucketMs);
      }

      return new MvDefinitionMetadata(mvTableNameWithType, baseTables, definedSql,
          partitionExprMaps, splitSpec);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to deserialize MvDefinitionMetadata from ZNRecord", e);
    }
  }

  /**
   * Specifies the time-column semantics needed for MV split queries: which source
   * column to filter on, its format, and the partition bucket size.
   *
   * <p>Thread-safety: instances are immutable.
   */
  public static class MvSplitSpec {
    private final String _sourceTimeColumn;
    private final String _sourceTimeFormat;
    private final long _bucketMs;

    public MvSplitSpec(String sourceTimeColumn, String sourceTimeFormat, long bucketMs) {
      _sourceTimeColumn = sourceTimeColumn;
      _sourceTimeFormat = sourceTimeFormat;
      _bucketMs = bucketMs;
    }

    public String getSourceTimeColumn() {
      return _sourceTimeColumn;
    }

    public String getSourceTimeFormat() {
      return _sourceTimeFormat;
    }

    public long getBucketMs() {
      return _bucketMs;
    }
  }
}
