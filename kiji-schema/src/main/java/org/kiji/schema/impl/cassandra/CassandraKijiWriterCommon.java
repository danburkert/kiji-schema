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
import java.nio.ByteBuffer;

import com.datastax.driver.core.Statement;
import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.HConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.TranslatedColumnName;
import org.kiji.schema.layout.impl.CellEncoderProvider;

/**
 * Contains code common to a TableWriter and BufferedWriter.
 */
class CassandraKijiWriterCommon {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiWriterCommon.class);

  private final CassandraAdmin mAdmin;

  private final CassandraKijiTable mTable;

  /**
   * Create an object for performing common write operations for a given table.
   *
   * @param table to which to write.
   */
  public CassandraKijiWriterCommon(CassandraKijiTable table) {
    mTable = table;
    mAdmin = mTable.getAdmin();
  }

  /**
   * Create a (bound) CQL statement that implements a Kiji put into a non-counter cell.
   *
   * @param entityId The entity ID of the destination cell.
   * @param column The column of the destination cell.
   * @param timestamp The timestamp of the destination cell.
   * @param value The bytes to be written to the destination cell.
   * @param <T> the type of the value to put.
   * @param encoderProvider for the value to put.
   * @return A CQL `Statement` that implements the put.
   * @throws IOException If something goes wrong (e.g., the column does not exist).
   */
  public <T> Statement getPutStatement(
      CellEncoderProvider encoderProvider,
      EntityId entityId,
      KijiColumnName column,
      long timestamp,
      T value) throws IOException {
    Preconditions.checkArgument(!isCounterColumn(column));

    // In Cassandra Kiji, a write to HConstants.LATEST_TIMESTAMP should be a write with the
    // current system time.
    if (timestamp == HConstants.LATEST_TIMESTAMP) {
      timestamp = System.currentTimeMillis();
    }

    int ttl = getTTL(column);

    final TranslatedColumnName translatedColumn =
        mTable.getKijiColumnNameTranslator().toTranslatedColumnName(column);

    final LocalityGroupLayout localityGroup = getFamily(column).getLocalityGroup();
    final CassandraTableName tableName =
        CassandraTableName.getKijiLocalityGroupTableName(mTable.getURI(), localityGroup);

    final ByteBuffer valueBytes =
        ByteBuffer.wrap(
            encoderProvider.getEncoder(column.getFamily(), column.getQualifier()).encode(value));

    return CQLUtils.getInsertStatement(
        mAdmin,
        mTable.getLayout(),
        tableName,
        entityId,
        translatedColumn,
        timestamp,
        valueBytes,
        ttl);
  }

  /**
   * Create a delete statement for a fully-qualified cell.
   *
   * @param entityId of the cell to delete.
   * @param column of cell to delete.
   * @param version of the cell to delete.
   * @return a statement that will delete the cell.
   * @throws IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteCellStatement(
      EntityId entityId,
      KijiColumnName column,
      long version
  ) throws IOException {
    final LocalityGroupLayout localityGroup = getFamily(column).getLocalityGroup();
    final CassandraTableName tableName =
        CassandraTableName.getKijiLocalityGroupTableName(mTable.getURI(), localityGroup);
    final TranslatedColumnName columnName =
        mTable.getKijiColumnNameTranslator().toTranslatedColumnName(column);

    return CQLUtils.getDeleteCellStatement(
        mAdmin,
        mTable.getLayout(),
        tableName,
        entityId,
        columnName,
        version);
  }

  /**
   * Create a delete statement for a column. May be fully qualified or unqualified.
   *
   * @param entityId of the cell to delete.
   * @param column to delete.
   * @return a statement that will delete the cell.
   * @throws IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteColumnStatement(EntityId entityId, KijiColumnName column)
      throws IOException {
    final LocalityGroupLayout localityGroup = getFamily(column).getLocalityGroup();
    final CassandraTableName tableName =
        CassandraTableName.getKijiLocalityGroupTableName(mTable.getURI(), localityGroup);
    final TranslatedColumnName columnName =
        mTable.getKijiColumnNameTranslator().toTranslatedColumnName(column);

    return CQLUtils.getDeleteColumnStatement(
        mAdmin,
        mTable.getLayout(),
        tableName,
        entityId,
        columnName);
  }

  /**
   * Create a delete statement for a counter column. May be fully qualified or unqualified.
   *
   * @param entityId of the cell to delete.
   * @param column to delete.
   * @return a statement that will delete the counter column.
   * @throws IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteCounterColumnStatement(EntityId entityId, KijiColumnName column)
      throws IOException {
    getFamily(column); // Check that the family is valid
    final CassandraTableName tableName =
        CassandraTableName.getKijiCounterTableName(mTable.getURI());
    final TranslatedColumnName columnName =
        mTable.getKijiColumnNameTranslator().toTranslatedColumnName(column);

    return CQLUtils.getDeleteColumnStatement(
        mAdmin,
        mTable.getLayout(),
        tableName,
        entityId,
        columnName);
  }

  /**
   * Create a delete statement for the counters within a row.
   *
   * @param entityId of the row to delete.
   * @return a statement that will delete the counters in the row.
   * @throws IOException if there is a problem creating the delete statement.
   */
  public Statement getDeleteCounterRowStatement(EntityId entityId) throws IOException {
    final CassandraTableName tableName =
        CassandraTableName.getKijiCounterTableName(mTable.getURI());
    return CQLUtils.getDeleteLocalityGroupStatement(mAdmin, mTable.getLayout(), tableName, entityId);
  }

  /**
   * Create a delete statement for a row within a locality group.
   *
   * @param entityId of the row to delete.
   * @param localityGroup to delete the row from.
   * @return a statement that will delete the row in the provided locality group.
   */
  public Statement getDeleteRowStatement(EntityId entityId, CassandraTableName localityGroup) {
    return CQLUtils.getDeleteLocalityGroupStatement(
        mAdmin,
        mTable.getLayout(),
        localityGroup,
        entityId);
  }

  /**
   * Returns the {@link FamilyLayout} of the column family which contains the provided Kiji column,
   * the call will fail with a {@link NoSuchColumnException} if the requested column family does
   * not exist.
   *
   * @param column whose locality group to return.
   * @return the locality group of the column.
   * @throws NoSuchColumnException if the column does not exist.
   */
  private FamilyLayout getFamily(KijiColumnName column) throws NoSuchColumnException {
    final FamilyLayout family = mTable.getLayout().getFamilyMap().get(column.getFamily());
    if (family == null) {
      throw new NoSuchColumnException(String.format("Family for column %s no found.", column));
    }
    return family;
  }

  /**
   * Check whether a given table contains a counter.
   *
   * @param column to check.
   * @return whether the column contains a counter.
   * @throws IOException if there is a problem reading the table layout.
   */
  public boolean isCounterColumn(KijiColumnName column) throws IOException {
    return mTable.getLayoutCapsule().getLayout().getCellSpec(column).isCounter();
  }

  /**
   * Get the TTL for a column family.
   *
   * @param column to check.
   * @return the TTL.
   */
  private int getTTL(KijiColumnName column) throws NoSuchColumnException {
    // Get the locality group name from the column name.
    return getFamily(column).getLocalityGroup().getDesc().getTtlSeconds();
  }
}
