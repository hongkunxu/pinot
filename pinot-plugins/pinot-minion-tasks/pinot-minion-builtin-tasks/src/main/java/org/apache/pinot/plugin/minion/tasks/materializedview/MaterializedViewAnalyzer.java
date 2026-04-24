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
package org.apache.pinot.plugin.minion.tasks.materializedview;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.core.common.MinionConstants.MaterializedViewTask;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.InferredTimeSpec;
import org.apache.pinot.plugin.minion.tasks.materializedview.timeexpr.TimeExprInferrer;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlParser;
import org.apache.pinot.sql.parsers.SqlCompilationException;


/**
 * Validates a materialized-view (MV) definition end-to-end using Calcite AST parsing.
 *
 * <p>Fail-fast: throws {@link IllegalStateException} on the first validation error.
 * Validations performed (in order):
 * <ol>
 *   <li>SQL syntax and semantic analysis via {@link CalciteSqlParser}</li>
 *   <li>Source (base) table existence and time-column configuration</li>
 *   <li>Source column existence for all identifiers referenced in the query</li>
 *   <li>MV schema column completeness against the SELECT output fields</li>
 *   <li>Aggregation function recognition</li>
 *   <li>Task config parameter validity (bucket period, buffer period, etc.)</li>
 *   <li>MV time column alignment: {@code segmentsConfig.timeColumnName} exists in the MV
 *       schema as a {@link DateTimeFieldSpec} and is produced by a SELECT expression</li>
 *   <li>MV time column format/granularity: the SELECT expression actually producing the MV
 *       time column is inferred (via {@link TimeExprInferrer}) and compared against the MV
 *       {@link DateTimeFieldSpec}'s declared format and granularity. Catches at create time
 *       silently incorrect setups (wrong format ⇒ wrong split-boundary conversion / wrong
 *       interval filtering at query time).</li>
 * </ol>
 *
 * <p>Thread-safety: all methods are stateless and static.
 */
public final class MaterializedViewAnalyzer {

  private static final String DEFAULT_BUCKET_PERIOD = "1d";

  private MaterializedViewAnalyzer() {
  }

  /**
   * Validates the MV definition and returns extracted metadata on success.
   *
   * @param definedSql         the user-defined SQL query for the MV
   * @param mvTableConfig      the MV table's {@link TableConfig}
   * @param mvSchema           the MV table's {@link Schema}
   * @param taskConfigs        task-type-specific configuration map
   * @param clusterInfoAccessor accessor for looking up source table config/schema
   * @return {@link AnalysisResult} with extracted source table name and select field names
   * @throws IllegalStateException on the first validation error encountered
   */
  public static AnalysisResult analyze(String definedSql, TableConfig mvTableConfig, Schema mvSchema,
      Map<String, String> taskConfigs, ClusterInfoAccessor clusterInfoAccessor) {

    // Step 4 first: cheap config checks that don't require parsing
    validateTaskConfigs(mvTableConfig, taskConfigs);

    // Step 1: SQL syntax and Pinot semantic validation
    PinotQuery pinotQuery = validateSqlSyntax(definedSql);

    // Step 1b: require a bounded, explicit LIMIT so the executor can detect truncation.
    // A silent default (e.g. 1M) would let an over-sized window be marked VALID and advance
    // coverageUpperMs with incomplete data, so this is a hard fail at definition time.
    validateExplicitLimit(pinotQuery);

    // Step 2: source table existence and time-column checks
    String sourceTableName = validateSourceTable(pinotQuery, definedSql, clusterInfoAccessor);

    // Source column existence
    validateSourceColumns(pinotQuery, sourceTableName, clusterInfoAccessor);

    // Step 3: MV schema column completeness (including dateTime columns)
    Set<String> selectFields = validateMvColumns(pinotQuery, mvSchema);

    // Step 5: extract and validate time column transformation mappings.
    // We need both the legacy "exprPretty -> mvCol" map (consumed by downstream metadata and by
    // Step 6) and a parallel "mvCol -> sourceExpr" map so Step 7 can locate the actual SELECT
    // expression producing each MV dateTime column without re-walking the SELECT list.
    PartitionExprData partitionExprData = extractPartitionExprData(pinotQuery, mvSchema);
    Map<String, String> partitionExprMaps = partitionExprData.exprStringToMvCol();

    // Step 6: MV time column (segmentsConfig.timeColumnName) must be wired to a SELECT-produced
    // dateTime column. Without this guard, a mismatch (e.g. timeColumnName=ts but SELECT only
    // produces date_trunc('DAY', ts) AS day) would only surface at task scheduling time via the
    // runtime Preconditions in MaterializedViewTaskGenerator#resolveMvTimeColumn / resolveMvTimeFormat.
    validateMvTimeColumnAlignment(mvTableConfig, mvSchema, partitionExprMaps);

    // Step 7: MV time column format/granularity must match what the SELECT expression actually
    // produces. Without this check, a mismatched DateTimeFieldSpec would only show up at query
    // time (wrong split-boundary conversion, wrong interval filtering). Step 6 has already
    // ensured the MV time column exists, is a DateTimeFieldSpec, and is in partitionExprMaps,
    // so the lookup below is guaranteed non-null.
    // TODO(mv): v1 only validates segmentsConfig.timeColumnName. Extend to all DateTimeFieldSpec
    // columns in a follow-up by looping over mvSchema.getDateTimeNames() and re-using the same
    // inferrer (loosening the "first arg must be base time column" rule there).
    validateMvTimeColumnFormat(mvTableConfig, mvSchema, sourceTableName,
        partitionExprData.mvColToSourceExpr(), clusterInfoAccessor);

    return new AnalysisResult(sourceTableName, selectFields, partitionExprMaps);
  }

  /**
   * Extracts the source table name from the SQL's FROM clause using Calcite AST parsing.
   * Unlike regex-based extraction, this handles quoted identifiers, comments, and complex SQL.
   *
   * @param sql the SQL query string
   * @return the source table name
   * @throws IllegalStateException if the SQL cannot be parsed or the table name cannot be extracted
   */
  public static String extractSourceTableName(String sql) {
    PinotQuery pinotQuery = validateSqlSyntax(sql);
    DataSource dataSource = pinotQuery.getDataSource();
    Preconditions.checkState(dataSource != null, "Could not extract data source from SQL: %s", sql);
    String tableName = dataSource.getTableName();
    Preconditions.checkState(tableName != null && !tableName.isEmpty(),
        "Could not extract source table name from SQL: %s", sql);
    return tableName;
  }

  // ---------------------------------------------------------------------------
  //  Step 1 — SQL syntax
  // ---------------------------------------------------------------------------

  private static PinotQuery validateSqlSyntax(String definedSql) {
    Preconditions.checkState(definedSql != null && !definedSql.isEmpty(), "definedSQL must be specified");
    try {
      return CalciteSqlParser.compileToPinotQuery(definedSql);
    } catch (SqlCompilationException e) {
      throw new IllegalStateException("Invalid SQL syntax: " + e.getMessage(), e);
    }
  }

  // ---------------------------------------------------------------------------
  //  Step 1b — LIMIT validation (AST-based)
  // ---------------------------------------------------------------------------

  /**
   * Requires the user's {@code definedSQL} to declare an explicit, bounded {@code LIMIT}.
   *
   * <p>We deliberately do not fall back to any silent default.  If the query actually produces
   * more rows than the declared LIMIT the executor will detect the saturation (via
   * {@code rows.size() >= LIMIT}) and fail the task, preventing incomplete data from being
   * marked VALID and from advancing {@code coverageUpperMs}.
   *
   * @throws IllegalStateException if LIMIT is missing, non-positive, or exceeds
   *     {@link MaterializedViewTask#MAX_MV_QUERY_LIMIT}
   */
  private static void validateExplicitLimit(PinotQuery pinotQuery) {
    Preconditions.checkState(pinotQuery.isSetLimit(),
        "MaterializedViewTask definedSQL must specify an explicit LIMIT clause. "
            + "A missing LIMIT would allow silent result truncation which could cause the MV "
            + "to be marked VALID with incomplete data.");
    int limit = pinotQuery.getLimit();
    Preconditions.checkState(limit > 0,
        "MaterializedViewTask definedSQL LIMIT must be strictly positive, got: %s", limit);
    Preconditions.checkState(limit <= MaterializedViewTask.MAX_MV_QUERY_LIMIT,
        "MaterializedViewTask definedSQL LIMIT %s exceeds the maximum allowed LIMIT %s. "
            + "Re-shape the query (e.g. narrower time bucket or stricter filters) so the "
            + "per-window result set fits, or request a higher cap explicitly.",
        limit, MaterializedViewTask.MAX_MV_QUERY_LIMIT);
  }

  /**
   * Extracts the declared {@code LIMIT} value from {@code definedSQL}.  Assumes the SQL has
   * already been accepted by {@link #analyze} (so LIMIT is guaranteed to be set).  Used by the
   * task generator to propagate the effective limit to the executor without re-parsing there.
   */
  public static int extractDeclaredLimit(String definedSql) {
    PinotQuery pinotQuery = validateSqlSyntax(definedSql);
    validateExplicitLimit(pinotQuery);
    return pinotQuery.getLimit();
  }

  // ---------------------------------------------------------------------------
  //  Step 2 — Source table
  // ---------------------------------------------------------------------------

  private static String validateSourceTable(PinotQuery pinotQuery, String definedSql,
      ClusterInfoAccessor clusterInfoAccessor) {
    DataSource dataSource = pinotQuery.getDataSource();
    Preconditions.checkState(dataSource != null, "Could not extract data source from SQL: %s", definedSql);

    String sourceTableName = dataSource.getTableName();
    Preconditions.checkState(sourceTableName != null && !sourceTableName.isEmpty(),
        "Could not extract source table name from SQL: %s", definedSql);

    String sourceTableWithType = resolveSourceTableWithType(sourceTableName, clusterInfoAccessor);

    TableConfig sourceTableConfig = clusterInfoAccessor.getTableConfig(sourceTableWithType);
    String timeColumn = sourceTableConfig.getValidationConfig().getTimeColumnName();
    Preconditions.checkState(timeColumn != null && !timeColumn.isEmpty(),
        "Source table '%s' has no time column configured", sourceTableName);

    Schema sourceSchema = clusterInfoAccessor.getTableSchema(sourceTableWithType);
    Preconditions.checkState(sourceSchema != null, "Schema not found for source table: %s", sourceTableName);

    DateTimeFieldSpec fieldSpec = sourceSchema.getSpecForTimeColumn(timeColumn);
    Preconditions.checkState(fieldSpec != null,
        "No DateTimeFieldSpec found for time column '%s' in source table '%s'", timeColumn, sourceTableName);

    return sourceTableName;
  }

  /**
   * Resolves the full table name with type suffix. Tries OFFLINE first, then REALTIME.
   */
  private static String resolveSourceTableWithType(String rawSourceTableName,
      ClusterInfoAccessor clusterInfoAccessor) {
    String offlineName = TableNameBuilder.OFFLINE.tableNameWithType(rawSourceTableName);
    if (clusterInfoAccessor.getTableConfig(offlineName) != null) {
      return offlineName;
    }
    String realtimeName = TableNameBuilder.REALTIME.tableNameWithType(rawSourceTableName);
    Preconditions.checkState(clusterInfoAccessor.getTableConfig(realtimeName) != null,
        "Source table '%s' does not exist (tried OFFLINE and REALTIME)", rawSourceTableName);
    return realtimeName;
  }

  // ---------------------------------------------------------------------------
  //  Source column existence
  // ---------------------------------------------------------------------------

  private static void validateSourceColumns(PinotQuery pinotQuery, String sourceTableName,
      ClusterInfoAccessor clusterInfoAccessor) {
    String sourceTableWithType = resolveSourceTableWithType(sourceTableName, clusterInfoAccessor);
    Schema sourceSchema = clusterInfoAccessor.getTableSchema(sourceTableWithType);
    Preconditions.checkState(sourceSchema != null, "Schema not found for source table: %s", sourceTableName);

    Set<String> sourceColumns = new HashSet<>(sourceSchema.getColumnNames());
    Set<String> referencedIdentifiers = new HashSet<>();
    for (Expression expr : pinotQuery.getSelectList()) {
      collectIdentifiers(expr, referencedIdentifiers);
    }
    if (pinotQuery.getGroupByList() != null) {
      for (Expression expr : pinotQuery.getGroupByList()) {
        collectIdentifiers(expr, referencedIdentifiers);
      }
    }

    for (String identifier : referencedIdentifiers) {
      Preconditions.checkState(sourceColumns.contains(identifier),
          "Column '%s' referenced in SQL does not exist in source table '%s'. Available columns: %s",
          identifier, sourceTableName, sourceColumns);
    }
  }

  // ---------------------------------------------------------------------------
  //  Step 3 — MV schema columns
  // ---------------------------------------------------------------------------

  private static Set<String> validateMvColumns(PinotQuery pinotQuery, Schema mvSchema) {
    List<Expression> selectList = pinotQuery.getSelectList();
    Preconditions.checkState(selectList != null && !selectList.isEmpty(), "SELECT list is empty");

    Set<String> selectFields = new HashSet<>();
    for (Expression expr : selectList) {
      String fieldName = extractOutputFieldName(expr);
      selectFields.add(fieldName);
    }

    // All MV schema columns (including dateTime columns) must be covered by SELECT
    Set<String> schemaColumns = new HashSet<>(mvSchema.getColumnNames());

    // Check 1: every MV schema column must be covered by a SELECT field
    for (String col : schemaColumns) {
      Preconditions.checkState(selectFields.contains(col),
          "MV schema column '%s' is not produced by any SELECT expression. SELECT fields: %s", col, selectFields);
    }

    // Check 2: every SELECT field must map to an MV schema column
    for (String field : selectFields) {
      Preconditions.checkState(schemaColumns.contains(field),
          "SELECT field '%s' does not match any column in the MV table schema. Schema columns: %s",
          field, schemaColumns);
    }

    // Check 3: aggregation function validity
    for (Expression expr : selectList) {
      validateAggregationFunctions(expr);
    }

    return selectFields;
  }

  /**
   * Extracts the output field name from a SELECT expression:
   * <ul>
   *   <li>Alias expressions ({@code expr AS alias}): returns the alias name</li>
   *   <li>Bare identifiers ({@code columnName}): returns the column name</li>
   *   <li>Aggregate/function without alias: throws</li>
   * </ul>
   */
  private static String extractOutputFieldName(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func != null) {
      if (func.getOperator().equals("as")) {
        Expression aliasExpr = func.getOperands().get(1);
        Preconditions.checkState(aliasExpr.getType() == ExpressionType.IDENTIFIER,
            "AS alias must be an identifier, got: %s", RequestUtils.prettyPrint(aliasExpr));
        return aliasExpr.getIdentifier().getName();
      }
      throw new IllegalStateException(
          "Expression '" + RequestUtils.prettyPrint(expr)
              + "' must have an AS alias to map to an MV schema column");
    }
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      return expr.getIdentifier().getName();
    }
    throw new IllegalStateException(
        "Unsupported expression type in SELECT list: " + RequestUtils.prettyPrint(expr));
  }

  /**
   * Recursively validates that all aggregation functions used are recognized by Pinot.
   */
  private static void validateAggregationFunctions(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func == null) {
      return;
    }
    String operator = func.getOperator();
    if (!operator.equals("as") && AggregationFunctionType.isAggregationFunction(operator)) {
      // Known aggregation — valid
    } else if (!operator.equals("as") && isLikelyAggregation(func)) {
      throw new IllegalStateException(
          "Aggregation function '" + operator + "' is not a recognized Pinot aggregation function");
    }
    if (func.getOperands() != null) {
      for (Expression operand : func.getOperands()) {
        validateAggregationFunctions(operand);
      }
    }
  }

  /**
   * Heuristic: a function call whose name doesn't match any known scalar/transform and appears
   * without a GROUP BY context is likely an unrecognized aggregation. For now we only flag
   * functions that CalciteSqlParser itself tagged as aggregation-like but are not in the enum.
   */
  private static boolean isLikelyAggregation(Function func) {
    return CalciteSqlParser.isAggregateExpression(wrapAsExpression(func));
  }

  private static Expression wrapAsExpression(Function func) {
    Expression expr = new Expression(ExpressionType.FUNCTION);
    expr.setFunctionCall(func);
    return expr;
  }

  // ---------------------------------------------------------------------------
  //  Step 4 — Task config parameters
  // ---------------------------------------------------------------------------

  private static void validateTaskConfigs(TableConfig mvTableConfig, Map<String, String> taskConfigs) {
    Preconditions.checkState(mvTableConfig.getTableType() == TableType.OFFLINE,
        "MaterializedViewTask only supports OFFLINE tables, got: %s", mvTableConfig.getTableType());

    String bucketPeriod = taskConfigs.getOrDefault(MaterializedViewTask.BUCKET_TIME_PERIOD_KEY, DEFAULT_BUCKET_PERIOD);
    try {
      long bucketMs = TimeUtils.convertPeriodToMillis(bucketPeriod);
      Preconditions.checkState(bucketMs > 0, "bucketTimePeriod must be positive, got: %s", bucketPeriod);
    } catch (Exception e) {
      throw new IllegalStateException("Invalid bucketTimePeriod '" + bucketPeriod + "': " + e.getMessage(), e);
    }

    String bufferPeriod = taskConfigs.get(MaterializedViewTask.BUFFER_TIME_PERIOD_KEY);
    if (bufferPeriod != null && !bufferPeriod.isEmpty()) {
      try {
        TimeUtils.convertPeriodToMillis(bufferPeriod);
      } catch (Exception e) {
        throw new IllegalStateException("Invalid bufferTimePeriod '" + bufferPeriod + "': " + e.getMessage(), e);
      }
    }

    String maxRecords = taskConfigs.get(MaterializedViewTask.MAX_NUM_RECORDS_PER_SEGMENT_KEY);
    if (maxRecords != null && !maxRecords.isEmpty()) {
      try {
        int value = Integer.parseInt(maxRecords);
        Preconditions.checkState(value > 0, "maxNumRecordsPerSegment must be positive, got: %d", value);
      } catch (NumberFormatException e) {
        throw new IllegalStateException(
            "Invalid maxNumRecordsPerSegment '" + maxRecords + "': must be a positive integer", e);
      }
    }
  }

  // ---------------------------------------------------------------------------
  //  Step 5 — Time column transformation mappings (partitionExprMaps)
  // ---------------------------------------------------------------------------

  /**
   * Extracts the mapping from base-table time column expressions to MV dateTime column names.
   *
   * <p>For each dateTime column in the MV schema, this method finds the corresponding SELECT
   * expression and records the transformation. The expression is the base-table side (e.g.,
   * {@code dateTimeConvert(ts, '1:MILLISECONDS:EPOCH', '1:DAYS:EPOCH', '1:DAYS')}) and the
   * value is the MV column identifier (e.g., {@code mvDay}).
   *
   * <p>If the query has a GROUP BY clause, this method also validates that each dateTime
   * expression appears in the GROUP BY list.
   *
   * @return map from expression string to MV column name
   */
  static Map<String, String> extractPartitionExprMaps(PinotQuery pinotQuery, Schema mvSchema) {
    return extractPartitionExprData(pinotQuery, mvSchema).exprStringToMvCol();
  }

  /**
   * Internal variant of {@link #extractPartitionExprMaps(PinotQuery, Schema)} that also
   * returns the {@code mvColName -> sourceExpression} mapping. Step 7 needs the live
   * {@link Expression} (not just the pretty-printed form) so it can run the time-expression
   * inferrer.
   *
   * <p>Both maps are produced in a single SELECT-list walk to avoid double-traversal.
   */
  static PartitionExprData extractPartitionExprData(PinotQuery pinotQuery, Schema mvSchema) {
    List<String> dateTimeNamesList = mvSchema.getDateTimeNames();
    if (dateTimeNamesList.isEmpty()) {
      return PartitionExprData.EMPTY;
    }

    Set<String> dateTimeNames = new HashSet<>(dateTimeNamesList);
    List<Expression> selectList = pinotQuery.getSelectList();
    Map<String, String> partitionExprMaps = new HashMap<>();
    Map<String, Expression> mvColToSourceExpr = new HashMap<>();

    for (Expression expr : selectList) {
      String outputName = extractOutputFieldName(expr);
      if (!dateTimeNames.contains(outputName)) {
        continue;
      }
      Expression sourceExpr = extractSourceExpression(expr);
      String exprString = RequestUtils.prettyPrint(sourceExpr);
      partitionExprMaps.put(exprString, outputName);
      mvColToSourceExpr.put(outputName, sourceExpr);
    }

    Preconditions.checkState(partitionExprMaps.size() == dateTimeNames.size(),
        "Not all MV dateTime columns are covered by SELECT expressions. "
            + "Expected dateTime columns: %s, found mappings: %s", dateTimeNames, partitionExprMaps);

    // If GROUP BY exists, verify that each dateTime expression is present in GROUP BY
    List<Expression> groupByList = pinotQuery.getGroupByList();
    if (groupByList != null && !groupByList.isEmpty()) {
      Set<String> groupByExprStrings = new HashSet<>();
      for (Expression gbExpr : groupByList) {
        groupByExprStrings.add(RequestUtils.prettyPrint(gbExpr));
      }
      for (Map.Entry<String, String> entry : partitionExprMaps.entrySet()) {
        Preconditions.checkState(groupByExprStrings.contains(entry.getKey()),
            "Time column expression '%s' (mapped to MV column '%s') must appear in GROUP BY "
                + "when a GROUP BY clause is present. Current GROUP BY: %s",
            entry.getKey(), entry.getValue(), groupByExprStrings);
      }
    }

    return new PartitionExprData(partitionExprMaps, mvColToSourceExpr);
  }

  /**
   * Extracts the source expression from a SELECT item, stripping any AS alias wrapper.
   */
  private static Expression extractSourceExpression(Expression expr) {
    Function func = expr.getFunctionCall();
    if (func != null && func.getOperator().equals("as")) {
      return func.getOperands().get(0);
    }
    return expr;
  }

  /**
   * Convenience overload that parses the SQL and extracts partition expression maps
   * without running full validation. Used by the task generator during cold-start.
   */
  public static Map<String, String> extractPartitionExprMaps(String definedSql, Schema mvSchema) {
    PinotQuery pinotQuery = validateSqlSyntax(definedSql);
    return extractPartitionExprMaps(pinotQuery, mvSchema);
  }

  // ---------------------------------------------------------------------------
  //  Step 6 — MV time column alignment
  // ---------------------------------------------------------------------------

  /**
   * Verifies at create/update time that the MV's {@code segmentsConfig.timeColumnName}
   * is actually produced by the {@code definedSql} and is a valid dateTime column in the
   * MV schema.
   *
   * <p>Without this guard, a misconfiguration (e.g. {@code timeColumnName} inherited from
   * the base table as {@code ts}, while SELECT only produces
   * {@code date_trunc('DAY', ts) AS day}) would only surface when the minion schedules a
   * task — the runtime {@code Preconditions} in
   * {@link MaterializedViewTaskGenerator}{@code #resolveMvTimeColumn} /
   * {@code #resolveMvTimeFormat} would then throw, failing the task instead of the
   * table configuration.
   *
   * <p>Enforced invariants:
   * <ol>
   *   <li>{@code timeColumnName} is set</li>
   *   <li>It exists in the MV schema</li>
   *   <li>It is registered as a {@link DateTimeFieldSpec} (not a dimension / metric)</li>
   *   <li>It is produced by some SELECT expression (present in {@code partitionExprMaps}'s
   *       values) — i.e. physically present in the MV</li>
   * </ol>
   *
   * <p>The stricter "format/granularity also match what the SELECT expression actually
   * produces" check is performed by Step 7 ({@link #validateMvTimeColumnFormat}), which
   * relies on the MV time column already passing invariants (1)–(4) here.
   */
  private static void validateMvTimeColumnAlignment(TableConfig mvTableConfig, Schema mvSchema,
      Map<String, String> partitionExprMaps) {
    String mvTimeColumn = mvTableConfig.getValidationConfig().getTimeColumnName();

    Preconditions.checkState(mvTimeColumn != null && !mvTimeColumn.isEmpty(),
        "MV table segmentsConfig.timeColumnName must be set (required for incremental refresh "
            + "and split-mode query rewrite).");

    Preconditions.checkState(mvSchema.getColumnNames().contains(mvTimeColumn),
        "MV time column '%s' does not exist in MV schema. Schema columns: %s",
        mvTimeColumn, mvSchema.getColumnNames());

    DateTimeFieldSpec fieldSpec = mvSchema.getSpecForTimeColumn(mvTimeColumn);
    Preconditions.checkState(fieldSpec != null,
        "MV time column '%s' is declared in segmentsConfig but is not a dateTime field in the MV "
            + "schema. Register it under dateTimeFieldSpecs with an explicit format.", mvTimeColumn);

    Preconditions.checkState(partitionExprMaps.containsValue(mvTimeColumn),
        "MV time column '%s' is not produced by any SELECT expression in definedSql. "
            + "The MV will not contain this column physically. "
            + "Either change segmentsConfig.timeColumnName to one of the time columns the "
            + "definedSql produces (candidates: %s), or add a SELECT alias that produces '%s'.",
        mvTimeColumn,
        partitionExprMaps.values().isEmpty() ? "<none>" : partitionExprMaps.values(),
        mvTimeColumn);
  }

  // ---------------------------------------------------------------------------
  //  Step 7 — MV time column format / granularity inference
  // ---------------------------------------------------------------------------

  /**
   * Verifies that the SELECT expression producing the MV {@code segmentsConfig.timeColumnName}
   * yields the same {@code format} (and, where derivable, {@code granularity}) as the MV
   * {@link DateTimeFieldSpec} declares. Without this check, a misconfiguration only surfaces
   * at query time as silently wrong split-boundary conversion or interval filtering.
   *
   * <p>Preconditions established by Steps 5–6: {@code mvTimeCol} is non-empty, exists in the MV
   * schema as a {@link DateTimeFieldSpec}, and {@code mvColToSourceExpr} contains an entry for
   * it. Therefore both lookups below are guaranteed non-null.
   */
  private static void validateMvTimeColumnFormat(TableConfig mvTableConfig, Schema mvSchema,
      String sourceTableName, Map<String, Expression> mvColToSourceExpr,
      ClusterInfoAccessor clusterInfoAccessor) {

    String mvTimeCol = mvTableConfig.getValidationConfig().getTimeColumnName();
    Expression sourceExpr = mvColToSourceExpr.get(mvTimeCol);
    DateTimeFieldSpec mvFieldSpec = mvSchema.getSpecForTimeColumn(mvTimeCol);
    // Defensive: Steps 5–6 guarantee these non-null. A null here would indicate an internal
    // ordering bug between steps, not a user-facing error, so fail loudly.
    Preconditions.checkState(sourceExpr != null,
        "Internal error: no SELECT source expression recorded for MV time column '%s'.", mvTimeCol);
    Preconditions.checkState(mvFieldSpec != null,
        "Internal error: MV time column '%s' has no DateTimeFieldSpec at format-validation step.",
        mvTimeCol);

    BaseTimeColumn baseTime = resolveBaseTimeColumn(sourceTableName, clusterInfoAccessor);

    InferredTimeSpec inferred = TimeExprInferrer.infer(sourceExpr, baseTime.name(), baseTime.fieldSpec());

    String mvFormat = mvFieldSpec.getFormat();
    String mvGranularity = mvFieldSpec.getGranularity();

    if (!inferred.formatMatcher().matches(mvFormat)) {
      throw new IllegalStateException(buildFormatMismatchMessage(mvTimeCol, "format",
          inferred.formatMatcher().describeExpected(), mvGranularity, mvFormat, mvGranularity,
          inferred.reason()));
    }

    String expectedGranularity = inferred.expectedGranularity();
    if (expectedGranularity != null) {
      if (!expectedGranularity.equals(mvGranularity)) {
        throw new IllegalStateException(buildFormatMismatchMessage(mvTimeCol, "granularity",
            inferred.formatMatcher().describeExpected(), expectedGranularity, mvFormat, mvGranularity,
            inferred.reason()));
      }
    } else {
      // toDateTime path: we don't infer granularity, but the MV must still declare one so that
      // downstream split-mode bucketing has a unit to work with.
      Preconditions.checkState(mvGranularity != null && !mvGranularity.isEmpty(),
          "MV time column '%s' uses toDateTime in the SELECT expression, which does not infer a "
              + "granularity. The MV DateTimeFieldSpec must still declare a non-empty 'granularity'. "
              + "reason: %s",
          mvTimeCol, inferred.reason());
    }
  }

  /**
   * Builds the unified expected/actual/reason mismatch message used by Step 7. {@code mismatchKind}
   * is "format" or "granularity" so the heading points the user at the offending field while the
   * body still prints both expected/actual format <i>and</i> granularity together — date_trunc and
   * dateTimeConvert have format/granularity coupling that is easy to misread in isolation.
   */
  private static String buildFormatMismatchMessage(String mvTimeCol, String mismatchKind,
      String expectedFormat, String expectedGranularity, String actualFormat, String actualGranularity,
      String reason) {
    return "MV time column '" + mvTimeCol + "' " + mismatchKind + " mismatch.\n"
        + "  expected: format=" + expectedFormat + ", granularity=" + expectedGranularity + "\n"
        + "  actual:   format=" + actualFormat + ", granularity=" + actualGranularity + "\n"
        + "  reason:   " + reason;
  }

  /**
   * Resolves the base table's primary time column name + its {@link DateTimeFieldSpec}.
   * Step 2 ({@link #validateSourceTable}) has already enforced that both exist, so any null
   * here would indicate an internal ordering bug.
   */
  private static BaseTimeColumn resolveBaseTimeColumn(String sourceTableName,
      ClusterInfoAccessor clusterInfoAccessor) {
    String sourceTableWithType = resolveSourceTableWithType(sourceTableName, clusterInfoAccessor);
    TableConfig sourceTableConfig = clusterInfoAccessor.getTableConfig(sourceTableWithType);
    String baseTimeColumn = sourceTableConfig.getValidationConfig().getTimeColumnName();
    Schema sourceSchema = clusterInfoAccessor.getTableSchema(sourceTableWithType);
    DateTimeFieldSpec baseFieldSpec = sourceSchema.getSpecForTimeColumn(baseTimeColumn);
    Preconditions.checkState(baseFieldSpec != null,
        "Internal error: base table '%s' time column '%s' resolved to null DateTimeFieldSpec at "
            + "format-validation step.", sourceTableName, baseTimeColumn);
    return new BaseTimeColumn(baseTimeColumn, baseFieldSpec);
  }

  /** Holder for the base table's primary time column name + its field spec. */
  private static final class BaseTimeColumn {
    private final String _name;
    private final DateTimeFieldSpec _fieldSpec;

    BaseTimeColumn(String name, DateTimeFieldSpec fieldSpec) {
      _name = name;
      _fieldSpec = fieldSpec;
    }

    String name() {
      return _name;
    }

    DateTimeFieldSpec fieldSpec() {
      return _fieldSpec;
    }
  }

  /**
   * Step-5 output: both the legacy {@code exprPretty -> mvCol} map (consumed by downstream
   * metadata + Step 6) and the {@code mvCol -> sourceExpr} map (consumed by Step 7).
   */
  static final class PartitionExprData {
    static final PartitionExprData EMPTY =
        new PartitionExprData(Collections.emptyMap(), Collections.emptyMap());

    private final Map<String, String> _exprStringToMvCol;
    private final Map<String, Expression> _mvColToSourceExpr;

    PartitionExprData(Map<String, String> exprStringToMvCol, Map<String, Expression> mvColToSourceExpr) {
      _exprStringToMvCol = exprStringToMvCol;
      _mvColToSourceExpr = mvColToSourceExpr;
    }

    Map<String, String> exprStringToMvCol() {
      return _exprStringToMvCol;
    }

    Map<String, Expression> mvColToSourceExpr() {
      return _mvColToSourceExpr;
    }
  }

  // ---------------------------------------------------------------------------
  //  Helpers
  // ---------------------------------------------------------------------------

  /**
   * Recursively collects all identifier names referenced in an expression tree,
   * skipping alias names (the right-hand side of AS).
   */
  private static void collectIdentifiers(Expression expr, Set<String> identifiers) {
    if (expr.getType() == ExpressionType.IDENTIFIER) {
      String name = expr.getIdentifier().getName();
      if (!"*".equals(name)) {
        identifiers.add(name);
      }
      return;
    }
    Function func = expr.getFunctionCall();
    if (func != null && func.getOperands() != null) {
      if (func.getOperator().equals("as")) {
        // Only collect from the actual expression (first operand), not the alias
        collectIdentifiers(func.getOperands().get(0), identifiers);
      } else {
        for (Expression operand : func.getOperands()) {
          collectIdentifiers(operand, identifiers);
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  //  AnalysisResult
  // ---------------------------------------------------------------------------

  /**
   * Holds extracted metadata from a successful analysis. Only returned when all validations pass.
   */
  public static class AnalysisResult {
    private final String _sourceTableName;
    private final Set<String> _selectFields;
    private final Map<String, String> _partitionExprMaps;

    AnalysisResult(String sourceTableName, Set<String> selectFields,
        Map<String, String> partitionExprMaps) {
      _sourceTableName = sourceTableName;
      _selectFields = selectFields;
      _partitionExprMaps = partitionExprMaps;
    }

    public String getSourceTableName() {
      return _sourceTableName;
    }

    public Set<String> getSelectFields() {
      return _selectFields;
    }

    public Map<String, String> getPartitionExprMaps() {
      return _partitionExprMaps;
    }
  }
}
