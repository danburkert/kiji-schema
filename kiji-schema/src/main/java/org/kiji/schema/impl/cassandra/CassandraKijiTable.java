/**
 * (c) Copyright 2012 WibiData, Inc.
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiReaderFactory;
import org.kiji.schema.KijiRegion;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableAnnotator;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.KijiWriterFactory;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.LayoutCapsule;
import org.kiji.schema.layout.impl.TableLayoutMonitor;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.VersionInfo;

/**
 * <p>A KijiTable that exposes the underlying Cassandra implementation.</p>
 *
 * <p>Within the internal Kiji code, we use this class so that we have
 * access to the Cassandra interface.  Methods that Kiji clients should
 * have access to should be added to org.kiji.schema.KijiTable.</p>
 */
@ApiAudience.Private
public final class CassandraKijiTable implements KijiTable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiTable.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + CassandraKijiTable.class.getName());

  /** The kiji instance this table belongs to. */
  private final CassandraKiji mKiji;

  /** URI of this table. */
  private final KijiURI mTableURI;

  /** States of a kiji table instance. */
  private static enum State {
    /**
     * Initialization begun but not completed.  Retain counter and DebugResourceTracker counters
     * have not been incremented yet.
     */
    UNINITIALIZED,
    /**
     * Finished initialization.  Both retain counters and DebugResourceTracker counters have been
     * incremented.  Resources are successfully opened and this KijiTable's methods may be used.
     */
    OPEN,
    /**
     * Closed.  Other methods are no longer supported.  Resources and connections have been closed.
     */
    CLOSED
  }

  /** Tracks the state of this kiji table. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** CassandraAdmin object that we use for interacting with the open C* session. */
  private final CassandraAdmin mAdmin;

  /** The factory for EntityIds. */
  private final EntityIdFactory mEntityIdFactory;

  /** Retain counter. When decreased to 0, the HBase KijiTable may be closed and disposed of. */
  private final AtomicInteger mRetainCount = new AtomicInteger(0);

  /** Writer factory for this table. */
  private final KijiWriterFactory mWriterFactory;

  /** Reader factory for this table. */
  private final KijiReaderFactory mReaderFactory;

  /** Monitor for the layout of this table. */
  private final TableLayoutMonitor mLayoutMonitor;

  /**
   * Construct an opened Kiji table stored in Cassandra.
   *
   * @param kiji The Kiji instance.
   * @param tableName The name of the Kiji user-space table to open.
   * @param admin The C* admin object.
   *
   * @throws java.io.IOException On a C* error.
   *     <p> Throws KijiTableNotFoundException if the table does not exist. </p>
   */
  CassandraKijiTable(
      CassandraKiji kiji,
      String tableName,
      CassandraAdmin admin,
      TableLayoutMonitor layoutMonitor)
      throws IOException {
    mKiji = kiji;
    mKiji.retain();

    mAdmin = admin;
    mTableURI = KijiURI.newBuilder(mKiji.getURI()).withTableName(tableName).build();
    LOG.debug("Opening Kiji table '{}' with client version '{}'.",
        mTableURI, VersionInfo.getSoftwareVersion());

    if (!mKiji.getTableNames().contains(tableName)) {
      throw new KijiTableNotFoundException(mTableURI);
    }

    mWriterFactory = new CassandraKijiWriterFactory(this);
    mReaderFactory = new CassandraKijiReaderFactory(this);

    mLayoutMonitor = layoutMonitor;
    mEntityIdFactory = createEntityIdFactory(mLayoutMonitor.getLayoutCapsule());

    // Table is now open and must be released properly:
    mRetainCount.set(1);

    DebugResourceTracker.get().registerResource(this, Debug.getStackTrace());

    final State oldState = mState.getAndSet(State.OPEN);
    Preconditions.checkState(oldState == State.UNINITIALIZED,
        "Cannot open KijiTable instance in state %s.", oldState);
  }

  /**
   * Constructs an Entity ID factory from a layout capsule.
   *
   * @param capsule Layout capsule to construct an entity ID factory from.
   * @return a new entity ID factory as described from the table layout.
   */
  private static EntityIdFactory createEntityIdFactory(final LayoutCapsule capsule) {
    final Object format = capsule.getLayout().getDesc().getKeysFormat();
    if (format instanceof RowKeyFormat) {
      return EntityIdFactory.getFactory((RowKeyFormat) format);
    } else if (format instanceof RowKeyFormat2) {
      return EntityIdFactory.getFactory((RowKeyFormat2) format);
    } else {
      throw new RuntimeException("Invalid Row Key format found in Kiji Table: " + format);
    }
  }

  /** {@inheritDoc} */
  @Override
  public EntityId getEntityId(Object... kijiRowKey) {
    return mEntityIdFactory.getEntityId(kijiRowKey);
  }

  /** {@inheritDoc} */
  @Override
  public Kiji getKiji() {
    return mKiji;
  }

  /** {@inheritDoc} */
  @Override
  public String getName() {
    return mTableURI.getTable();
  }

  /** {@inheritDoc} */
  @Override
  public KijiURI getURI() {
    return mTableURI;
  }

  /**
   * Register a layout consumer that must be updated before this table will report that it has
   * completed a table layout update.  Sends the first update immediately before returning.
   *
   * @param consumer the LayoutConsumer to be registered.
   * @throws java.io.IOException in case of an error updating the LayoutConsumer.
   */
  public void registerLayoutConsumer(LayoutConsumer consumer)
      throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot register a new layout consumer to a KijiTable in state %s.", state);
    mLayoutMonitor.registerLayoutConsumer(consumer);
  }

  /**
   * Unregister a layout consumer so that it will not be updated when this table performs a layout
   * update.  Should only be called when a consumer is closed.
   *
   * @param consumer the LayoutConsumer to unregister.
   */
  public void unregisterLayoutConsumer(LayoutConsumer consumer) {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot unregister a layout consumer from a KijiTable in state %s.", state);
    mLayoutMonitor.unregisterLayoutConsumer(consumer);
  }

  /**
   * Get the TableLayoutMonitor which is associated with this KijiTable.
   *
   * @return the TableLayoutMonitor associated with this KijiTable.
   */
  public TableLayoutMonitor getTableLayoutMonitor() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get a table layout monitor from a KijiTable in state %s.", state);
    return mLayoutMonitor;
  }

  /**
   * {@inheritDoc}
   * If you need both the table layout and a column name translator within a single short lived
   * operation, you should use {@link #getLayoutCapsule()} to ensure consistent state.
   */
  @Override
  public KijiTableLayout getLayout() {
    return getLayoutCapsule().getLayout();
  }

  /**
   * Get the column name translator for the current layout of this table.  Do not cache this object.
   * If you need both the table layout and a column name translator within a single short lived
   * operation, you should use {@link #getLayoutCapsule()} to ensure consistent state.
   * @return the column name translator for the current layout of this table.
   */
  public KijiColumnNameTranslator getKijiColumnNameTranslator() {
    return getLayoutCapsule().getKijiColumnNameTranslator();
  }

  /**
   * Get the LayoutCapsule containing a snapshot of the state of this table's layout and
   * corresponding ColumnNameTranslator.  Do not cache this object or its contents.
   * @return a layout capsule representing the current state of this table's layout.
   */
  public LayoutCapsule getLayoutCapsule() {
    return mLayoutMonitor.getLayoutCapsule();
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableReader openTableReader() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open a table reader on a KijiTable in state %s.", state);
    try {
      return CassandraKijiTableReader.create(this);
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableWriter openTableWriter() {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open a table writer on a KijiTable in state %s.", state);
    try {
      return new CassandraKijiTableWriter(this);
    } catch (IOException ioe) {
      throw new KijiIOException(ioe);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiReaderFactory getReaderFactory() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the reader factory for a KijiTable in state %s.", state);
    return mReaderFactory;
  }

  /** {@inheritDoc} */
  @Override
  public KijiWriterFactory getWriterFactory() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the writer factory for a KijiTable in state %s.", state);
    return mWriterFactory;
  }

  /**
   * Unlike HBase, Cassandra does not have a notion of regions, so this implementation always throws
   * an {@link java.lang.UnsupportedOperationException}
   *
   * @return never.
   * @throws IOException never.
   * @throws UnsupportedOperationException always.
   */
  @Override
  public List<KijiRegion> getRegions() throws IOException {
    throw new UnsupportedOperationException("Cassandra-backed Kiji tables do not have regions.");
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableAnnotator openTableAnnotator() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the TableAnnotator for a table in state: %s.", state);
    return new CassandraKijiTableAnnotator(this);
  }

  /**
   * Releases the resources used by this table.
   *
   * @throws java.io.IOException on I/O error.
   */
  private void closeResources() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiTable instance %s in state %s.", this, oldState);

    LOG.debug("Closing CassandraKijiTable '{}'.", mTableURI);

    mKiji.release();
    LOG.debug("HBaseKijiTable '{}' closed.", mTableURI);
  }

  /** {@inheritDoc} */
  @Override
  public KijiTable retain() {
    final int counter = mRetainCount.getAndIncrement();
    Preconditions.checkState(counter >= 1,
        "Cannot retain a closed KijiTable %s: retain counter was %s.", mTableURI, counter);
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void release() throws IOException {
    final int counter = mRetainCount.decrementAndGet();
    Preconditions.checkState(counter >= 0,
        "Cannot release closed KijiTable %s: retain counter is now %s.", mTableURI, counter);
    if (counter == 0) {
      closeResources();
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean equals(Object obj) {
    if (null == obj) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (!getClass().equals(obj.getClass())) {
      return false;
    }
    final KijiTable other = (KijiTable) obj;

    // Equal if the two tables have the same URI:
    return mTableURI.equals(other.getURI());
  }

  /** {@inheritDoc} */
  @Override
  public int hashCode() {
    return mTableURI.hashCode();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(CassandraKijiTable.class)
        .add("id", System.identityHashCode(this))
        .add("uri", mTableURI)
        .add("retain_counter", mRetainCount.get())
        .add("layout_id", getLayoutCapsule().getLayout().getDesc().getLayoutId())
        .add("state", mState.get())
        .toString();
  }

  /**
   * Getter method for this instances C* admin.
   *
   * Necessary now so that readers and writers can execute CQL commands.
   *
   * @return The C* admin object for this table.
   */
  public CassandraAdmin getAdmin() {
    return mAdmin;
  }
}
