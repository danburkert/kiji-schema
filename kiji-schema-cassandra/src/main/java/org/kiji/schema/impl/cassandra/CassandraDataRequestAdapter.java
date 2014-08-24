/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;

/**
 * Wraps a KijiDataRequest to expose methods that generate meaningful objects in Cassandra land.
 */
@ApiAudience.Private
public class CassandraDataRequestAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraDataRequestAdapter.class);

  /** The wrapped KijiDataRequest. */
  private final KijiDataRequest mKijiDataRequest;

  /** The translator for generating Cassandra column names. */
  private final CassandraColumnNameTranslator mColumnNameTranslator;

  /**
   * Creates a new CassandraDataRequestAdapter for a given data request using a given
   * ColumnNameTranslator.
   *
   * @param kijiDataRequest The data request to adapt for Cassandra.
   * @param translator The name translator for getting Cassandra column names.
   */
  public CassandraDataRequestAdapter(
      final KijiDataRequest kijiDataRequest,
      final CassandraColumnNameTranslator translator
  ) {
    mKijiDataRequest = kijiDataRequest;
    mColumnNameTranslator = translator;
  }

  /**
   * Executes the Cassandra scan request and returns a list of `ResultSet` objects.
   *
   * @param table The table to scan.
   * @param kijiScannerOptions Options for the scan.
   * @return A list of `ResultSet`s from executing the scan.
   * @throws java.io.IOException if there is a problem talking to Cassandra.
   */
  public ListMultimap<CassandraTableName, ResultSetFuture> doScan(
      final CassandraKijiTable table,
      final CassandraKijiScannerOptions kijiScannerOptions
    ) throws IOException {
    return queryCassandraTables(table, null, false, kijiScannerOptions);
  }

  /**
   * Perform a Cassandra CQL "SELECT" statement (like an HBase Get).  Will ignore any columns with
   * paging enabled (use `doPagedGet` for requests with paging).
   *
   * @param table The table from which to fetch data.
   * @param entityId The entity ID for the row from which to fetch data.
   * @return A list of `ResultSet`s from executing the get.
   * @throws java.io.IOException if there is a problem executing the get.
   */
  public ListMultimap<CassandraTableName, ResultSetFuture> doGet(
      final CassandraKijiTable table,
      final EntityId entityId
  ) throws IOException {
    return queryCassandraTables(table, entityId, false);
  }

  /**
   * Perform a Cassandra CQL "SELECT" statement (like an HBase Get).  Will fetch only columns with
   * paging enabled (use `doGet` for requests without paging).
   *
   * @param table The table from which to fetch data.
   * @param entityId The entity ID for the row from which to fetch data.
   * @return A list of `ResultSet`s from executing the get.
   * @throws java.io.IOException if there is a problem executing the get.
   */
  public ListMultimap<CassandraTableName, ResultSetFuture> doPagedGet(
      final CassandraKijiTable table,
      final EntityId entityId
  ) throws IOException {
    return queryCassandraTables(table, entityId, true);
  }

  /**
   * Query a Cassandra table for a get or for a scan.
   *
   * @param table The Cassandra Kiji table to scan.
   * @param entityId Make null if this is a scan over all entity IDs (not currently supporting
   *     entity ID ranges).
   * @param pagingEnabled If true, all columns in the data request should be paged.  If false, skip
   *                      any paged columns in the data request.
   * @return A list of results for the Cassandra query.
   * @throws java.io.IOException if there is a problem executing the scan.
   */
  private ListMultimap<CassandraTableName, ResultSetFuture> queryCassandraTables(
      final CassandraKijiTable table,
      final EntityId entityId,
      final boolean pagingEnabled
  ) throws IOException {
    return queryCassandraTables(table, entityId, pagingEnabled, null);
  }

  /**
   * Query a Cassandra table for a get or for a scan.
   *
   * @param table The Cassandra Kiji table to scan.
   * @param entityId Make null if this is a scan over all entity IDs (not currently supporting
   *     entity ID ranges).
   * @param pagingEnabled If true, all columns in the data request should be paged.  If false, skip
   *                      any paged columns in the data request.
   * @param scannerOptions Cassandra-specific scanner options (set to `null` if this is not a scan).
   * @return A list of results for the Cassandra query.
   * @throws java.io.IOException if there is a problem executing the scan.
   */
  private ListMultimap<CassandraTableName, ResultSetFuture> queryCassandraTables(
      CassandraKijiTable table,
      EntityId entityId,
      boolean pagingEnabled,
      CassandraKijiScannerOptions scannerOptions
  ) throws IOException {
    // TODO: Or figure out how to combine multiple SELECT statements into a single RPC (IntraVert?).
    // TODO: Or just combine everything into a single SELECT statement!

    boolean bIsScan = (null == entityId);
    if (!bIsScan) {
      Preconditions.checkArgument(null == scannerOptions);
    }

    // Cannot do a scan with paging.
    Preconditions.checkArgument(!(pagingEnabled && bIsScan));

    // A single Kiji data request can result in many Cassandra queries, so we use asynchronous IO
    // and keep track of all of the futures that will contain results from Cassandra.
    ListMultimap<CassandraTableName, ResultSetFuture> futures = ArrayListMultimap.create();

    Set<CassandraTableName> pagedTables = Sets.newHashSet();
    Set<CassandraTableName> nonPagedTables = Sets.newHashSet();

    // For now, to keep things simple, we have a separate request for each column, even if there
    // are multiple columns of interest in the same column family that we could potentially put
    // together into a single query.
    for (Column columnRequest : mKijiDataRequest.getColumns()) {

      // Translate the Kiji columnRequest name.
      KijiColumnName column = columnRequest.getColumnName();
      CassandraColumnName cassandraColumn = mColumnNameTranslator.toCassandraColumnName(column);

      final Collection<CassandraTableName> cassandraTables =
          getColumnCassandraTables(table.getURI(), table.getLayout(), column);

      if (!pagingEnabled && columnRequest.isPagingEnabled()) {
        pagedTables.addAll(cassandraTables);
        // The user will have to use an explicit KijiPager to get this data.
        continue;
      }

      nonPagedTables.addAll(cassandraTables);

      // TODO: Optimize these queries such that we need only one RPC per columnRequest family.
      // (Right now a data request that asks for "info:foo" and "info:bar" would trigger two
      // separate session.execute(statement) commands.

      for (CassandraTableName cassandraTable : cassandraTables) {
        if (bIsScan) {
          Statement statement = CQLUtils.getColumnScanStatement(
              table.getLayout(),
              cassandraTable,
              cassandraColumn,
              mKijiDataRequest,
              columnRequest,
              scannerOptions);
          if (pagingEnabled) {
            statement.setFetchSize(columnRequest.getPageSize());
          }
          futures.put(cassandraTable, table.getAdmin().executeAsync(statement));
        } else {
          Statement statement =
              CQLUtils.getColumnGetStatement(
                  table.getLayout(),
                  cassandraTable,
                  entityId,
                  cassandraColumn,
                  mKijiDataRequest,
                  columnRequest);
          futures.put(cassandraTable, table.getAdmin().executeAsync(statement));
        }
      }
    }

    if (bIsScan) {
      // If this is a paged scan, you need to make sure that you execute at least one SELECT
      // statement, just to get back every entity ID. If you do not do so, then a user could
      // create a scanner with a data request that has entirely paged columns, and you would never
      // execute a SELECT query (because we wait to execute paged SELECT queries) and so you
      // would never get an iterator back with any row keys at all!  Eek!
      pagedTables.removeAll(nonPagedTables);

      for (CassandraTableName tableName : pagedTables) {
        final Statement statement = CQLUtils.getEntityIDScanStatement(table.getLayout(), tableName);
        futures.put(tableName, table.getAdmin().executeAsync(statement));
      }
    }

    return futures;
  }

  /**
   * Get the Cassandra table names which contain a Kiji column (fully-qualified or family).
   *
   * @param tableURI The URI of the table.
   * @param layout The layout of the table.
   * @param column The column to get the Cassandra table names for.
   * @return The Cassandra table names for a Kiji column.
   */
  private static Collection<CassandraTableName> getColumnCassandraTables(
      final KijiURI tableURI,
      final KijiTableLayout layout,
      final KijiColumnName column
  ) {
    final FamilyLayout familyLayout = layout.getFamilyMap().get(column.getFamily());
    Preconditions.checkArgument(familyLayout != null,
        "Kiji table %s does not contain family '%s'.", tableURI, column.getFamily());

    boolean containsCounter = false;
    boolean containsNonCounter = false;

    if (column.isFullyQualified()) {
      final ColumnLayout columnLayout = familyLayout.getColumnMap().get(column.getQualifier());
      Preconditions.checkArgument(columnLayout != null,
          "Kiji table %s does not contain column '%s' in family '%s'.",
          tableURI, column.getQualifier(), column.getFamily());

      if (columnLayout.getDesc().getColumnSchema().getType() == SchemaType.COUNTER) {
        containsCounter = true;
      } else {
        containsNonCounter = true;
      }
    } else if (familyLayout.isMapType()) {
      if (familyLayout.getDesc().getMapSchema().getType() == SchemaType.COUNTER) {
        containsCounter = true;
      } else {
        containsNonCounter = true;
      }
    } else {
      for (ColumnLayout columnLayout : familyLayout.getColumns()) {
        if (columnLayout.getDesc().getColumnSchema().getType() == SchemaType.COUNTER) {
          containsCounter = true;
        } else {
          containsNonCounter = true;
        }
        if (containsCounter && containsNonCounter) {
          break;
        }
      }
    }

    final List<CassandraTableName> tables = Lists.newArrayListWithCapacity(2);
    if (containsCounter) {
      tables.add(CassandraTableName.getCounterTableName(tableURI));
    }
    if (containsNonCounter) {
      tables.add(
          CassandraTableName.getLocalityGroupTableName(
              tableURI,
              familyLayout.getLocalityGroup().getId()));
    }
    return tables;
  }
}
