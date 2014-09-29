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

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.avro.RowKeyComponent;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;

/**
 * A cache for prepared CQL statements. This is a stateful object which is scoped to a single
 * Kiji Cassandra Kiji table (but may be shared among multiple
 * {@link org.kiji.schema.impl.cassandra.CassandraKijiTable} instances). Responsibilities:
 *
 * <ul>
 *   <li>Generating and preparing CQL statements</li>
 *   <li>Caching prepared CQL statements</li>
 *   <li>Applying arguments to prepared CQL statements</li>
 * </ul>
 *
 * <p>
 *    This class is stateful and uses the {@link org.kiji.schema.layout.KijiTableLayout} of it's
 *    owning Kiji table to create queries. Typically caching a {@code KijiTableLayout} is a bad
 *    practice, because Kiji table layouts can change. Fortunately, only the row-key format is
 *    needed from the table layout. Because the row-key format can not change once a table is
 *    created, it is safe to use the row-key format without registering a table layout update
 *    handler.
 * </p>
 */
public class CQLStatementCache {
  private static final Logger LOG = LoggerFactory.getLogger(CQLStatementCache.class);

  /**
   * Prefix added to Kiji entity ID component names to make the Cassandra column name. Adding a
   * prefix is necessary to avoid name conflicts between the entity ID component names and other
   * Cassandra column names.
   */
  private static final String ENTITY_ID_PREFIX = "eid_";

  private final RowKeyFormat2 mRowKeyFormat;

  /** Cassandra primary key columns belonging to the Kiji Entity ID. */
  private final List<String> mEntityIDColumns;

  /** Cassandra partition key columns. */
  private final List<String> mPartitionKeyColumns;

  /** Cassandra clustering columns belonging to the Kiji Entity ID. */
  private final List<String> mEntityIDClusteringColumns;

  private final Session mSession;

  /**
   * Marker interface for statement 'keys'.  These keys are used to lookup the statement in the
   * cache. Statement keys must override {@code hashCode} and {@code equals}, and must take into
   * account the type of the key when doing these operations. Unfortunately there is not way to
   * encode this requirement in the type system.
   */
  private interface StatementKey {
    /**
     * Create an unprepared CQL statement corresponding to the key. The unprepared statement will
     * automatically be prepared and cached by {@code mCache}.
     *
     * @return An unprepared CQL statement corresponding to the key.
     */
    RegularStatement createUnpreparedStatement();
  }

  private final LoadingCache<StatementKey, PreparedStatement> mCache =
      CacheBuilder
          .newBuilder()
          .expireAfterAccess(60, TimeUnit.MINUTES) // Avoid caching one-off queries forever
          .build(
              new CacheLoader<StatementKey, PreparedStatement>() {
                /** {@inheritDoc} */
                @Override
                public PreparedStatement load(final StatementKey key) {
                  return mSession.prepare(key.createUnpreparedStatement());
                }
              });

  /**
   * Create a new CQL statement cache for a Kiji table.
   *
   * @param session The Cassandra connection.
   * @param rowKeyFormat The table's row key format.
   */
  public CQLStatementCache(final Session session, final RowKeyFormat2 rowKeyFormat) {
    mSession = session;
    mRowKeyFormat = rowKeyFormat;
    // Cassandra Kiji only supports RowKeyFormat2
    switch (rowKeyFormat.getEncoding()) {
      case RAW: {
        final String name = CQLUtils.RAW_KEY_COL;
        mEntityIDColumns = ImmutableList.of(name);
        break;
      }
      case FORMATTED: {
        final ImmutableList.Builder<String> entityIDColumns = ImmutableList.builder();
        for (final RowKeyComponent component : mRowKeyFormat.getComponents()) {
          entityIDColumns.add(ENTITY_ID_PREFIX + component.getName());
        }
        mEntityIDColumns = entityIDColumns.build();
        break;
      }
      default:
        throw new IllegalArgumentException(
            String.format("Unknown row key encoding %s.", mRowKeyFormat.getEncoding()));
    }

    mPartitionKeyColumns =
        mEntityIDColumns.subList(0, rowKeyFormat.getRangeScanStartIndex());

    mEntityIDClusteringColumns =
        mEntityIDColumns.subList(rowKeyFormat.getRangeScanStartIndex(),  mEntityIDColumns.size());
  }

  /**
   * Get the entity ID component values from an Entity ID.
   *
   * @param entityID The entity ID.
   * @return The entity ID's component values.
   */
  private List<Object> getEntityIDComponents(
      final EntityId entityID
  ) {
    switch (mRowKeyFormat.getEncoding()) {
      case RAW: {
        return ImmutableList.<Object>of(ByteBuffer.wrap(entityID.getHBaseRowKey()));
      }
      case FORMATTED: {
        return entityID.getComponents();
      }
      default: throw new IllegalArgumentException(
          String.format("Unknown row key encoding %s.", mRowKeyFormat.getEncoding()));
    }
  }

  /*************************************************************************************************
   * Get Statement
   ************************************************************************************************/

  /**
   * Create a statement for retrieving a column in single row of a Cassandra Kiji table.
   *
   * @param tableName The Cassandra locality group table.
   * @param entityId The Kiji entity ID.
   * @param column The translated Kiji column name.
   * @param dataRequest The data request defining overall query parameters.
   * @param columnRequest The column request defining the column to retrieve.
   * @return A statement for querying the column.
   */
  public Statement createGetStatement(
      final CassandraTableName tableName,
      final EntityId entityId,
      final CassandraColumnName column,
      final KijiDataRequest dataRequest,
      final Column columnRequest
  ) {
    Preconditions.checkArgument(entityId.getComponents().size() == mEntityIDColumns.size(),
        "Entity ID components mismatch. entity ID components: {}, entity ID columns: {}",
        entityId.getComponents(), mEntityIDColumns);

    // Retrieve the prepared statement from the cache

    final boolean isQualifiedGet = column.containsQualifier();
    final boolean hasMaxTimestamp =
        column.containsQualifier() && dataRequest.getMaxTimestamp() != Long.MAX_VALUE;
    final boolean hasMinTimestamp =
        column.containsQualifier() && dataRequest.getMinTimestamp() != 0;

    final GetStatementKey key =
        new GetStatementKey(tableName, isQualifiedGet, hasMaxTimestamp, hasMinTimestamp);

    final PreparedStatement statement = mCache.getUnchecked(key);

    // Bind the parameters to the prepared statement

    // The extra 5 slots are for the family, qualifier, min/max timestamps, and max versions
    final List<Object> values = Lists.newArrayListWithCapacity(mEntityIDColumns.size() + 5);

    values.addAll(getEntityIDComponents(entityId));
    values.add(column.getFamilyBuffer());

    if (column.containsQualifier()) {
      values.add(column.getQualifierBuffer());
    }

    if (hasMaxTimestamp) {
      values.add(dataRequest.getMaxTimestamp());
    }

    if (hasMinTimestamp) {
      values.add(dataRequest.getMinTimestamp());
    }

    // Only limit the number of versions if this is a qualified get. Family gets will need to limit
    // versions on the client side.
    if (isQualifiedGet) {
      values.add(columnRequest.getMaxVersions());
    }

    final Statement boundStatement = statement.bind(values.toArray());

    if (columnRequest.getPageSize() != 0) {
      boundStatement.setFetchSize(columnRequest.getPageSize());
    }

    return boundStatement;
  }

  /**
   * A key containing all of the information necessary to create a prepared statement for a get.
   */
  private final class GetStatementKey implements StatementKey {
    private final CassandraTableName mTable;
    private final boolean mIsQualifiedGet;
    private final boolean mHasMaxTimestamp;
    private final boolean mHashMinTimestamp;

    /**
     * Create a new get statement key. This key contains all of the information necessary to
     * create an unbound statement for the scan.
     *
     * @param table The Cassandra table name.
     * @param isQualifiedGet Whether the get is fully qualified (true) or for an entire family
     *    (false).
     * @param hasMaxTimestamp Whether the includes a max timestamp. Only valid with qualified gets.
     * @param hashMinTimestamp Whether the get includes a min timestamp. Only valid with qualified
     *    gets.
     */
    private GetStatementKey(
        final CassandraTableName table,
        final boolean isQualifiedGet,
        final boolean hasMaxTimestamp,
        final boolean hashMinTimestamp
    ) {
      mTable = table;
      mIsQualifiedGet = isQualifiedGet;
      mHasMaxTimestamp = hasMaxTimestamp;
      mHashMinTimestamp = hashMinTimestamp;
      Preconditions.checkState(!mHasMaxTimestamp || mIsQualifiedGet,
          "Max timestamp may only be set on fully-qualified gets. Key: %s.", this);
      Preconditions.checkState(!mHashMinTimestamp || mIsQualifiedGet,
          "Min timestamp may only be set on fully-qualified gets. Key: %s.", this);
    }

    /**
     * Get the table name.
     *
     * @return The table name.
     */
    public CassandraTableName getTable() {
      return mTable;
    }

    /** {@inheritDoc} */
    @Override
    public RegularStatement createUnpreparedStatement() {
      final Select select =
          select()
              .all()
              .from(mTable.getKeyspace(), mTable.getTable());

      for (final String componentColumn : mEntityIDColumns) {
        select.where(eq(componentColumn, bindMarker()));
      }

      select.where(eq(CQLUtils.FAMILY_COL, bindMarker()));

      if (mIsQualifiedGet) {
        select.where(eq(CQLUtils.QUALIFIER_COL, bindMarker()));
        if (mHasMaxTimestamp) {
          select.where(lt(CQLUtils.VERSION_COL, bindMarker()));
        }
        if (mHashMinTimestamp) {
          select.where(gte(CQLUtils.VERSION_COL, bindMarker()));
        }
        select.limit(bindMarker());
      }

      return select;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return com.google.common.base.Objects.toStringHelper(this)
          .add("mTable", mTable)
          .add("mIsQualifiedGet", mIsQualifiedGet)
          .add("mHasMaxTimestamp", mHasMaxTimestamp)
          .add("mHashMinTimestamp", mHashMinTimestamp)
          .toString();
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(
          GetStatementKey.class,
          mTable,
          mIsQualifiedGet,
          mHasMaxTimestamp,
          mHashMinTimestamp);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final GetStatementKey other = (GetStatementKey) obj;
      return Objects.equal(mTable, other.mTable)
          && Objects.equal(mIsQualifiedGet, other.mIsQualifiedGet)
          && Objects.equal(mHasMaxTimestamp, other.mHasMaxTimestamp)
          && Objects.equal(mHashMinTimestamp, other.mHashMinTimestamp);
    }
  }

  /*************************************************************************************************
   * Entity ID Scan Statement
   ************************************************************************************************/

  public Statement createEntityIDScanStatement(
      final CassandraTableName tableName,
      final CassandraKijiScannerOptions options
  ) {

    // Retrieve the prepared statement from the cache

    // We can only use the 'DISTINCT' optimization if all entity ID components are part of the
    // partition key. CQL does not allow DISTINCT over non partition-key columns.
    final boolean useDistinct = mEntityIDClusteringColumns.isEmpty();

    final EntityIDScanKey key =
        new EntityIDScanKey(
            tableName,
            options.hasStartToken(),
            options.hasStopToken(),
            useDistinct);
    final PreparedStatement statement = mCache.getUnchecked(key);

    // Bind the parameters to the prepared statement

    // slots are for the min/max token
    final List<Object> values = Lists.newArrayListWithCapacity(2);

    if (options.hasStartToken()) {
      values.add(options.getStartToken());
    }

    if (options.hasStopToken()) {
      values.add(options.getStopToken());
    }

    return statement.bind(values.toArray()).setFetchSize(CQLUtils.ENTITY_ID_BATCH_SIZE);
  }

  private final class EntityIDScanKey implements StatementKey {
    private final CassandraTableName mTable;
    private final boolean mHasStartToken;
    private final boolean mHasStopToken;
    private final boolean mUseDistinct;

    /**
     * Create a new entity ID scan statement key.
     *
     * @param table The Cassandra table name.
     * @param hasStartToken Whether the scan contains a start token.
     * @param hasStopToken Whether the scan contains a stop token.
     */
    private EntityIDScanKey(
        final CassandraTableName table,
        final boolean hasStartToken,
        final boolean hasStopToken,
        final boolean useDistinct
    ) {
      mTable = table;
      mHasStartToken = hasStartToken;
      mHasStopToken = hasStopToken;
      mUseDistinct = useDistinct;
    }

    /** {@inheritDoc} */
    @Override
    public RegularStatement createUnpreparedStatement() {
      final String tokenColumn =
          String.format("token(%s)", CQLUtils.COMMA_JOINER.join(mPartitionKeyColumns));

      final Select.Selection selection = select();
      selection.column(tokenColumn);

      for (final String column : mEntityIDColumns) {
        selection.column(column);
      }

      if (mUseDistinct) {
        selection.distinct();
      }

      final Select select = selection.from(mTable.getKeyspace(), mTable.getTable());

      if (mHasStartToken) {
        select.where(gte(tokenColumn, bindMarker()));
      }

      if (mHasStopToken) {
        select.where(lt(tokenColumn, bindMarker()));
      }

      return select;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("mTable", mTable)
          .add("mHasStartToken", mHasStartToken)
          .add("mHasStopToken", mHasStopToken)
          .add("mUseDistinct", mHasStopToken)
          .toString();
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
      return Objects.hashCode(
          EntityIDScanKey.class,
          mTable,
          mHasStartToken,
          mHasStopToken,
          mUseDistinct);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      final EntityIDScanKey other = (EntityIDScanKey) obj;
      return Objects.equal(this.mTable, other.mTable)
          && Objects.equal(this.mHasStartToken, other.mHasStartToken)
          && Objects.equal(this.mHasStopToken, other.mHasStopToken)
          && Objects.equal(this.mUseDistinct, other.mUseDistinct);
    }
  }
}
