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
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.DecodedCell;
import org.kiji.schema.EntityId;
import org.kiji.schema.KConstants;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.impl.DefaultKijiCellEncoderFactory;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellEncoderProvider;
import org.kiji.schema.layout.impl.LayoutCapsule;

/**
 * Makes modifications to a Kiji table by sending requests directly to Cassandra from the local
 * client.
 *
 */
@ApiAudience.Private
public final class CassandraKijiTableWriter implements KijiTableWriter {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiTableWriter.class);

  /** The kiji table instance. */
  private final CassandraKijiTable mTable;

  /** C* Admin instance, used for executing CQL commands. */
  private final CassandraAdmin mAdmin;

  /** Contains shared code with BufferedWriter. */
  private final CassandraKijiWriterCommon mWriterCommon;

  /** States of a writer instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this writer. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** Processes layout update from the KijiTable to which this writer writes. */
  private final InnerLayoutUpdater mInnerLayoutUpdater = new InnerLayoutUpdater();

  /**
   * All state which should be modified atomically to reflect an update to the underlying table's
   * layout.
   */
  private volatile WriterLayoutCapsule mWriterLayoutCapsule = null;

  /**
   * A container for all writer state which should be modified atomically to reflect an update to
   * the underlying table's layout.
   */
  public static final class WriterLayoutCapsule {
    private final CellEncoderProvider mCellEncoderProvider;
    private final KijiTableLayout mLayout;
    private final KijiColumnNameTranslator mTranslator;

    /**
     * Default constructor.
     *
     * @param cellEncoderProvider the encoder provider to store in this container.
     * @param layout the table layout to store in this container.
     * @param translator the column name translator to store in this container.
     */
    public WriterLayoutCapsule(
        final CellEncoderProvider cellEncoderProvider,
        final KijiTableLayout layout,
        final KijiColumnNameTranslator translator) {
      mCellEncoderProvider = cellEncoderProvider;
      mLayout = layout;
      mTranslator = translator;
    }

    /**
     * Get the column name translator from this container.
     *
     * @return the column name translator from this container.
     */
    public KijiColumnNameTranslator getColumnNameTranslator() {
      return mTranslator;
    }

    /**
     * Get the table layout from this container.
     *
     * @return the table layout from this container.
     */
    public KijiTableLayout getLayout() {
      return mLayout;
    }

    /**
     * get the encoder provider from this container.
     *
     * @return the encoder provider from this container.
     */
    public CellEncoderProvider getCellEncoderProvider() {
      return mCellEncoderProvider;
    }
  }

  /** Provides for the updating of this Writer in response to a table layout update. */
  private final class InnerLayoutUpdater implements LayoutConsumer {
    /** {@inheritDoc} */
    @Override
    public void update(final LayoutCapsule capsule) throws IOException {
      final State state = mState.get();
      if (state == State.CLOSED) {
        LOG.debug("Writer is closed: ignoring layout update.");
        return;
      }
      final CellEncoderProvider provider = new CellEncoderProvider(
          mTable.getURI(),
          capsule.getLayout(),
          mTable.getKiji().getSchemaTable(),
          DefaultKijiCellEncoderFactory.get());
      // If the capsule is null this is the initial setup and we do not need a log message.
      if (mWriterLayoutCapsule != null) {
        LOG.debug(
            "Updating layout used by KijiTableWriter: {} for table: {} from version: {} to: {}",
            this,
            mTable.getURI(),
            mWriterLayoutCapsule.getLayout().getDesc().getLayoutId(),
            capsule.getLayout().getDesc().getLayoutId());
      } else {
        LOG.debug("Initializing KijiTableWriter: {} for table: {} with table layout version: {}",
            this,
            mTable.getURI(),
            capsule.getLayout().getDesc().getLayoutId());
      }
      // Normally we would atomically flush and update mWriterLayoutCapsule here,
      // but since this writer is unbuffered, the flush is unnecessary
      mWriterLayoutCapsule = new WriterLayoutCapsule(
          provider,
          capsule.getLayout(),
          capsule.getKijiColumnNameTranslator());
    }
  }

  /**
   * Creates a non-buffered kiji table writer that sends modifications directly to Kiji.
   *
   * @param table A kiji table.
   * @throws java.io.IOException on I/O error.
   */
  public CassandraKijiTableWriter(CassandraKijiTable table) throws IOException {
    mTable = table;
    mTable.registerLayoutConsumer(mInnerLayoutUpdater);
    Preconditions.checkState(mWriterLayoutCapsule != null,
        "KijiTableWriter for table: %s failed to initialize.", mTable.getURI());

    mAdmin = mTable.getAdmin();

    // Retain the table only when everything succeeds.
    mTable.retain();
    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTableWriter instance in state %s.", oldState);
    mWriterCommon = new CassandraKijiWriterCommon(mTable);
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, T value)
      throws IOException {
    final KijiColumnName column = new KijiColumnName(family, qualifier);
    // Check whether this col is a counter; if so, do special counter write.
    if (mWriterCommon.isCounterColumn(column)) {
      doCounterPut(entityId, column, (Long) value);
    } else {
      put(entityId, family, qualifier, System.currentTimeMillis(), value);
    }
  }

  /** {@inheritDoc} */
  @Override
  public <T> void put(EntityId entityId, String family, String qualifier, long timestamp, T value)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put cell to KijiTableWriter instance %s in state %s.", this, state);
    final KijiColumnName column = new KijiColumnName(family, qualifier);

    // Check whether this col is a counter; if so, do special counter write.
    if (mWriterCommon.isCounterColumn(column)) {
      throw new UnsupportedOperationException("Cannot specify a timestamp during a counter put.");
    }

    Statement putStatement = mWriterCommon.getPutStatement(
        mWriterLayoutCapsule.mCellEncoderProvider, entityId, column, timestamp, value);
    mAdmin.execute(putStatement);
  }

  // ----------------------------------------------------------------------------------------------
  // Counter set, get, increment.

  /**
   * Get the value of a counter.
   *
   * @param entityId of the row containing the counter.
   * @param column containing the counter.
   * @return the value of the counter.
   * @throws IOException if there is a problem reading the counter value.
   */
  private long getCounterValue(
      EntityId entityId,
      KijiColumnName column
  ) throws IOException {
    // Get a reference to the full name of the C* table for this column.
    CassandraTableName cTableName = CassandraTableName.getKijiCounterTableName(mTable.getURI());
    final KijiColumnNameTranslator translator = mWriterLayoutCapsule.getColumnNameTranslator();

    Statement statement = CQLUtils.getColumnGetStatement(
        mAdmin,
        mTable.getLayout(),
        cTableName,
        entityId,
        translator.toTranslatedColumnName(column),
        null,
        null,
        null,
        1);
    ResultSet resultSet = mAdmin.execute(statement);

    List<Row> readCounterResults = resultSet.all();

    long currentCounterValue;
    if (readCounterResults.isEmpty()) {
      currentCounterValue = 0; // Uninitialized counter, effectively a counter at 0.
    } else if (1 == readCounterResults.size()) {
      currentCounterValue = readCounterResults.get(0).getLong(CQLUtils.VALUE_COL);
    } else {
      // TODO: Handle this appropriately, this should never happen!
      throw new KijiIOException("Should not have multiple values for a counter!");
    }
    return currentCounterValue;
  }

  /**
   * Increment the value of a counter.
   *
   * @param entityId of the row containing the counter.
   * @param column to increment.
   * @param counterIncrement by which to increment the counter.
   * @throws IOException if there is a problem incrementing the counter value.
   */
  private void incrementCounterValue(
      EntityId entityId,
      KijiColumnName column,
      long counterIncrement
  ) throws IOException {
    LOG.info("Incrementing the counter by " + counterIncrement);

    // Get a reference to the full name of the C* table for this column.
    CassandraTableName cTableName = CassandraTableName.getKijiCounterTableName(mTable.getURI());

    final KijiColumnNameTranslator translator = mWriterLayoutCapsule.getColumnNameTranslator();

    mAdmin.execute(
        CQLUtils.getIncrementCounterStatement(
            mAdmin,
            mTable.getLayout(),
            cTableName,
            entityId,
            translator.toTranslatedColumnName(column),
            counterIncrement));
  }

  /**
   * Perform a put to a counter.
   * @param entityId of the row containing the counter.
   * @param column to increment.
   * @param value to write to the counter.
   * @throws IOException if there is a problem writing the counter.
   */
  private void doCounterPut(
      EntityId entityId,
      KijiColumnName column,
      long value) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot put cell to KijiTableWriter instance %s in state %s.", this, state);

    // TODO: this is really, very, dangerously wrong.  Fix.

    // Read back the current value of the counter.
    long currentCounterValue = getCounterValue(entityId, column);
    LOG.info("Current value of counter is " + currentCounterValue);
    // Increment the counter appropriately to get the new value.
    long counterIncrement = value - currentCounterValue;
    incrementCounterValue(entityId, column, counterIncrement);

  }

  /** {@inheritDoc} */
  @Override
  public KijiCell<Long> increment(EntityId entityId, String family, String qualifier, long amount)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot increment cell to KijiTableWriter instance %s in state %s.", this, state);
    final KijiColumnName column = new KijiColumnName(family, qualifier);

    if (!mWriterCommon.isCounterColumn(column)) {
      throw new UnsupportedOperationException(
          String.format("Column '%s:%s' is not a counter", family, qualifier));
    }

    // Increment the counter appropriately to get the new value.
    incrementCounterValue(entityId, column, amount);

    // Read back the current value of the counter.
    long currentCounterValue = getCounterValue(entityId, column);
    LOG.info("Value of counter after increment is " + currentCounterValue);

    final DecodedCell<Long> counter =
        new DecodedCell<Long>(null, currentCounterValue);
    return new KijiCell<Long>(family, qualifier, KConstants.CASSANDRA_COUNTER_TIMESTAMP, counter);
  }

  // ----------------------------------------------------------------------------------------------
  // Deletes

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete row while KijiTableWriter %s is in state %s.", this, state);
    // TODO: Could check whether this family has an non-counter / counter columns before delete.
    // TODO: Should we wait for these calls to complete before returning?
    for (CassandraTableName tableName :
        CassandraTableName.getKijiLocalityGroupTableNames(mTable.getURI(), mTable.getLayout())) {
      mAdmin.executeAsync(mWriterCommon.getDeleteRowStatement(entityId, tableName));
    }
    mAdmin.executeAsync(mWriterCommon.getDeleteCounterRowStatement(entityId));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteRow(EntityId entityId, long upToTimestamp) throws IOException {
    throw new UnsupportedOperationException(
        "Cannot delete with up-to timestamp in Cassandra Kiji.");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete family while KijiTableWriter %s is in state %s.", this, state);
    final KijiColumnName column = new KijiColumnName(family, null);

    // TODO: Could check whether this family has an non-counter / counter columns before delete.
    // TODO: Should we wait for these calls to complete before returning?
    mAdmin.executeAsync(mWriterCommon.getDeleteColumnStatement(entityId, column));
    mAdmin.executeAsync(mWriterCommon.getDeleteCounterColumnStatement(entityId, column));
  }

  /** {@inheritDoc} */
  @Override
  public void deleteFamily(EntityId entityId, String family, long upToTimestamp)
      throws IOException {
    throw new UnsupportedOperationException(
        "Cannot delete with up-to timestamp in Cassandra Kiji.");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete column while KijiTableWriter %s is in state %s.", this, state);
    final KijiColumnName column = new KijiColumnName(family, null);

    Statement statement;
    if (mWriterCommon.isCounterColumn(column)) {
      statement = mWriterCommon.getDeleteCounterColumnStatement(entityId, column);
    } else {
      statement = mWriterCommon.getDeleteColumnStatement(entityId, column);
    }

    mAdmin.execute(statement);
  }

  /** {@inheritDoc} */
  @Override
  public void deleteColumn(EntityId entityId, String family, String qualifier, long upToTimestamp)
      throws IOException {
    throw new UnsupportedOperationException(
        "Cannot delete with up-to timestamp in Cassandra Kiji.");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier) throws IOException {
    throw new UnsupportedOperationException(
        "Cannot delete only most-recent version of a cell in Cassandra Kiji.");
  }

  /** {@inheritDoc} */
  @Override
  public void deleteCell(EntityId entityId, String family, String qualifier, long timestamp)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot delete cell while KijiTableWriter %s is in state %s.", this, state);
    final KijiColumnName column = new KijiColumnName(family, null);

    if (mWriterCommon.isCounterColumn(column)) {
      throw new UnsupportedOperationException(
          "Cannot delete specific version of counter column in Cassandra Kiji.");
    }

    mAdmin.execute(mWriterCommon.getDeleteCellStatement(entityId, column, timestamp));
  }

  // ----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public void flush() throws IOException {
    LOG.debug("KijiTableWriter does not need to be flushed.");
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiTableWriter instance %s in state %s.", this, oldState);
    mTable.unregisterLayoutConsumer(mInnerLayoutUpdater);
    // TODO: May need a close call here for reference counting of Cassandra tables.
    //mHTable.close();
    mTable.release();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraKijiTableWriter.class)
        .add("id", System.identityHashCode(this))
        .add("table", mTable.getURI())
        .add("layout-version", mWriterLayoutCapsule.getLayout().getDesc().getLayoutId())
        .add("state", mState)
        .toString();
  }
}
