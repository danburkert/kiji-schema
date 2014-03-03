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

package org.kiji.schema.impl;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.kiji.schema.layout.impl.TableLayoutMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.EntityIdFactory;
import org.kiji.schema.InternalKijiError;
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
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.avro.RowKeyFormat2;
import org.kiji.schema.avro.RowKeyFormat;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.ColumnNameTranslator;
import org.kiji.schema.layout.impl.TableLayoutMonitor.LayoutCapsule;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.LayoutTracker;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.LayoutUpdateHandler;
import org.kiji.schema.layout.impl.ZooKeeperMonitor;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.DebugResourceTracker;
import org.kiji.schema.util.JvmId;
import org.kiji.schema.util.VersionInfo;

/**
 * <p>A KijiTable that exposes the underlying HBase implementation.</p>
 *
 * <p>Within the internal Kiji code, we use this class so that we have
 * access to the HTable interface.  Methods that Kiji clients should
 * have access to should be added to org.kiji.schema.KijiTable.</p>
 */
@ApiAudience.Private
public final class HBaseKijiTable implements KijiTable {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKijiTable.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + HBaseKijiTable.class.getName());

  /** The kiji instance this table belongs to. */
  private final HBaseKiji mKiji;

  /** The name of this table (the Kiji name, not the HBase name). */
  private final String mName;

  /** URI of this table. */
  private final KijiURI mTableURI;

  /** States of a kiji table instance. */
  private static enum State {
    UNINITIALIZED,
    OPEN,
    CLOSED
  }

  /** Tracks the state of this kiji table. */
  private final AtomicReference<State> mState = new AtomicReference<State>(State.UNINITIALIZED);

  /** String representation of the call stack at the time this object is constructed. */
  private final String mConstructorStack;

  /** HTableInterfaceFactory for creating new HTables associated with this KijiTable. */
  private final HTableInterfaceFactory mHTableFactory;

  /** The factory for EntityIds. */
  private final EntityIdFactory mEntityIdFactory;

  /** Retain counter. When decreased to 0, the HBase KijiTable may be closed and disposed of. */
  private final AtomicInteger mRetainCount = new AtomicInteger(0);

  /** Configuration object for new HTables. */
  private final Configuration mConf;

  /** Writer factory for this table. */
  private final KijiWriterFactory mWriterFactory;

  /** Reader factory for this table. */
  private final KijiReaderFactory mReaderFactory;

  /** Pool of HTable connections. Safe for concurrent access. */
  private final KijiHTablePool mHTablePool;

  /** Name of the HBase table backing this Kiji table. */
  private final String mHBaseTableName;

  private final TableLayoutMonitor mLayoutMonitor;

  /**
   * Construct an opened Kiji table stored in HBase.
   *
   * @param kiji The Kiji instance.
   * @param name The name of the Kiji user-space table to open.
   * @param conf The Hadoop configuration object.
   * @param htableFactory A factory that creates HTable objects.
   *
   * @throws IOException On an HBase error.
   *     <p> Throws KijiTableNotFoundException if the table does not exist. </p>
   */
  HBaseKijiTable(
      HBaseKiji kiji,
      String name,
      Configuration conf,
      TableLayoutMonitor layoutMonitor,
      HTableInterfaceFactory htableFactory)
      throws IOException {

    mKiji = kiji;
    mName = name;
    mHTableFactory = htableFactory;
    mConf = conf;
    mTableURI = KijiURI.newBuilder(mKiji.getURI()).withTableName(mName).build();
    LOG.debug("Opening Kiji table '{}' with client version '{}'.",
        mTableURI, VersionInfo.getSoftwareVersion());
    mHBaseTableName =
        KijiManagedHBaseTableName.getKijiTableName(mTableURI.getInstance(), mName).toString();

    if (!mKiji.getTableNames().contains(mName)) {
      throw new KijiTableNotFoundException(mTableURI);
    }

    mLayoutMonitor = layoutMonitor;

    mWriterFactory = new HBaseKijiWriterFactory(this);
    mReaderFactory = new HBaseKijiReaderFactory(this);

    mEntityIdFactory = createEntityIdFactory(layoutMonitor.getLayoutCapsule());

    mHTablePool = new KijiHTablePool(mName, (HBaseKiji)getKiji(), mHTableFactory);

    // Retain the Kiji instance only if open succeeds:
    mKiji.retain();

    // Table is now open and must be released properly:
    mRetainCount.set(1);

    mConstructorStack = (CLEANUP_LOG.isDebugEnabled()) ? Debug.getStackTrace() : null;
    DebugResourceTracker.get().registerResource(this, mConstructorStack);

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
    return mName;
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
   * @throws IOException in case of an error updating the LayoutConsumer.
   */
  public void registerLayoutConsumer(LayoutConsumer consumer) throws IOException {
    mLayoutMonitor.registerConsumer(consumer);
  }

  /**
   * Unregister a layout consumer so that it will not be updated when this table performs a layout
   * update.  Should only be called when a consumer is closed.
   *
   * @param consumer the LayoutConsumer to unregister.
   */
  public void unregisterLayoutConsumer(LayoutConsumer consumer) {
    mLayoutMonitor.unregisterConsumer(consumer);
  }

  /**
   * <p>
   * Get the set of registered layout consumers.  All layout consumers should be updated using
   * {@link LayoutConsumer#update(org.kiji.schema.impl.HBaseKijiTable.LayoutCapsule)} before this
   * table reports that it has successfully update its layout.
   * </p>
   * <p>
   * This method is package private for testing purposes only.  It should not be used externally.
   * </p>
   * @return the set of registered layout consumers.
   */
  Set<LayoutConsumer> getLayoutConsumers() {
    return mLayoutMonitor.getLayoutConsumers();
  }

  /**
   * Update all registered LayoutConsumers with a new KijiTableLayout.
   *
   * This method is package private for testing purposes only.  It should not be used externally.
   *
   * @param layout the new KijiTableLayout with which to update consumers.
   * @throws IOException in case of an error updating LayoutConsumers.
   */
  void updateLayoutConsumers(KijiTableLayout layout) throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot update layout consumers for a KijiTable in state %s.", state);
    layout.setSchemaTable(mKiji.getSchemaTable());
    ZooKeeperMonitor.setTableLayout(mKiji.getZKClient(), mTableURI, layout.getDesc().getLayoutId());
  }

  /**
   * Opens a new connection to the HBase table backing this Kiji table.
   *
   * <p> The caller is responsible for properly closing the connection afterwards. </p>
   * <p>
   *   Note: this does not necessarily create a new HTable instance, but may instead return
   *   an already existing HTable instance from a pool managed by this HBaseKijiTable.
   *   Closing a pooled HTable instance internally moves the HTable instance back into the pool.
   * </p>
   *
   * @return A new HTable associated with this KijiTable.
   * @throws IOException in case of an error.
   */
  public HTableInterface openHTableConnection() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot open an HTable connection for a KijiTable in state %s.", state);
    return mHTablePool.getTable();
  }

  /**
   * {@inheritDoc}
   * If you need both the table layout and a column name translator within a single short lived
   * operation, you should use {@link #getLayoutCapsule()} to ensure consistent state.
   */
  @Override
  public KijiTableLayout getLayout() {
    return mLayoutMonitor.getTableLayout();
  }

  /**
   * Get the column name translator for the current layout of this table.  Do not cache this object.
   * If you need both the table layout and a column name translator within a single short lived
   * operation, you should use {@link #getLayoutCapsule()} to ensure consistent state.
   * @return the column name translator for the current layout of this table.
   */
  public ColumnNameTranslator getColumnNameTranslator() {
    return mLayoutMonitor.getColumnNameTranslator();
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
      return HBaseKijiTableReader.create(this);
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
      return new HBaseKijiTableWriter(this);
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
   * Return the regions in this table as a list.
   *
   * <p>This method was copied from HFileOutputFormat of 0.90.1-cdh3u0 and modified to
   * return KijiRegion instead of ImmutableBytesWritable.</p>
   *
   * @return An ordered list of the table regions.
   * @throws IOException on I/O error.
   */
  @Override
  public List<KijiRegion> getRegions() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the regions for a KijiTable in state %s.", state);
    final HBaseAdmin hbaseAdmin = ((HBaseKiji) getKiji()).getHBaseAdmin();
    final HTableInterface htable = mHTableFactory.create(mConf,  mHBaseTableName);
    try {
      final List<HRegionInfo> regions = hbaseAdmin.getTableRegions(htable.getTableName());
      final List<KijiRegion> result = Lists.newArrayList();

      // If we can get the concrete HTable, we can get location information.
      if (htable instanceof HTable) {
        LOG.debug("Casting HTableInterface to an HTable.");
        final HTable concreteHBaseTable = (HTable) htable;
        for (HRegionInfo region: regions) {
          List<HRegionLocation> hLocations =
              concreteHBaseTable.getRegionsInRange(region.getStartKey(), region.getEndKey());
          result.add(new HBaseKijiRegion(region, hLocations));
        }
      } else {
        LOG.warn("Unable to cast HTableInterface {} to an HTable.  "
            + "Creating Kiji regions without location info.", getURI());
        for (HRegionInfo region: regions) {
          result.add(new HBaseKijiRegion(region));
        }
      }

      return result;

    } finally {
      htable.close();
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiTableAnnotator openTableAnnotator() throws IOException {
    final State state = mState.get();
    Preconditions.checkState(state == State.OPEN,
        "Cannot get the TableAnnotator for a table in state: %s.", state);
    return new HBaseKijiTableAnnotator(this);
  }

  /**
   * Releases the resources used by this table.
   *
   * @throws IOException on I/O error.
   */
  private void closeResources() throws IOException {
    final State oldState = mState.getAndSet(State.CLOSED);
    Preconditions.checkState(oldState == State.OPEN,
        "Cannot close KijiTable instance %s in state %s.", this, oldState);
    LOG.debug("Closing HBaseKijiTable '{}'.", this);
    mHTablePool.close();
    mKiji.release();
    DebugResourceTracker.get().unregisterResource(this);

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
  protected void finalize() throws Throwable {
    final State state = mState.get();
    if (state != State.CLOSED) {
      CLEANUP_LOG.warn("Finalizing unreleased KijiTable {} in state {}.", this, state);
      if (CLEANUP_LOG.isDebugEnabled()) {
        CLEANUP_LOG.debug(
            "HBaseKijiTable '{}' was constructed through:\n{}",
            mTableURI, mConstructorStack);
      }
      closeResources();
    }
    super.finalize();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(HBaseKijiTable.class)
        .add("id", System.identityHashCode(this))
        .add("uri", mTableURI)
        .add("retain_counter", mRetainCount.get())
            // TODO: figure out how to handle toString after the provider has been closed
//        .add("layout_id", getLayoutCapsule().getLayout().getDesc().getLayoutId())
        .add("state", mState.get())
        .toString();
  }

  /**
   * We know that all KijiTables are really HBaseKijiTables
   * instances.  This is a convenience method for downcasting, which
   * is common within the internals of Kiji code.
   *
   * @param kijiTable The Kiji table to downcast to an HBaseKijiTable.
   * @return The given Kiji table as an HBaseKijiTable.
   */
  public static HBaseKijiTable downcast(KijiTable kijiTable) {
    if (!(kijiTable instanceof HBaseKijiTable)) {
      // This should really never happen.  Something is seriously
      // wrong with Kiji code if we get here.
      throw new InternalKijiError(
          "Found a KijiTable object that was not an instance of HBaseKijiTable.");
    }
    return (HBaseKijiTable) kijiTable;
  }

  /**
   * Creates a new HFile loader.
   *
   * @param conf Configuration object for the HFile loader.
   * @return the new HFile loader.
   */
  private static LoadIncrementalHFiles createHFileLoader(Configuration conf) {
    try {
      return new LoadIncrementalHFiles(conf); // throws Exception
    } catch (Exception exn) {
      throw new InternalKijiError(exn);
    }
  }

  /**
   * Loads partitioned HFiles directly into the regions of this Kiji table.
   *
   * @param hfilePath Path of the HFiles to load.
   * @throws IOException on I/O error.
   */
  public void bulkLoad(Path hfilePath) throws IOException {
    final LoadIncrementalHFiles loader = createHFileLoader(mConf);
    try {
      // LoadIncrementalHFiles.doBulkLoad() requires an HTable instance, not an HTableInterface:
      final HTable htable = (HTable) mHTableFactory.create(mConf, mHBaseTableName);
      try {
        final List<Path> hfilePaths = Lists.newArrayList();

        // Try to find any hfiles for partitions within the passed in path
        final FileStatus[] hfiles = FileSystem.get(mConf).globStatus(new Path(hfilePath, "*"));
        for (FileStatus hfile : hfiles) {
          String partName = hfile.getPath().getName();
          if (!partName.startsWith("_") && partName.endsWith(".hfile")) {
            Path partHFile = new Path(hfilePath, partName);
            hfilePaths.add(partHFile);
          }
        }
        if (hfilePaths.isEmpty()) {
          // If we didn't find any parts, add in the passed in parameter
          hfilePaths.add(hfilePath);
        }
        for (Path path : hfilePaths) {
          loader.doBulkLoad(path, htable);
          LOG.info("Successfully loaded: " + path.toString());
        }
      } finally {
        htable.close();
      }
    } catch (TableNotFoundException tnfe) {
      throw new InternalKijiError(tnfe);
    }
  }
}
