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
package org.kiji.schema.layout.impl;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * TableLayoutMonitor provides three services for users of table layouts:
 *
 *  1) it acts as a KijiTableLayout cache which is automatically refreshed when the table layout is
 *     updated.
 *  2) it allows LayoutConsumer instances to register to recieve a callback when the table layout
 *     changes.
 *  3) it registers as a table user in ZooKeeper, and keeps that registration up-to-date with the
 *     oldest version of table layout being used by registered LayoutConsumers.
 *
 * This class is thread-safe with the following caveat:
 *  * LayoutConsumer instances who register to receive callbacks *must* keep a strong reference to
 *    this TableLayoutMonitor for as long as the registration should remain active. This is either
 *    for the life of the object if it is never unregistered, or until
 *    {@link #unregisterLayoutConsumer(LayoutConsumer)} is called.
 */
public class TableLayoutMonitor implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TableLayoutMonitor.class);

  private final KijiURI mTableURI;

  private final KijiSchemaTable mSchemaTable;

  private final KijiMetaTable mMetaTable;

  private final ZooKeeperMonitor.LayoutTracker mLayoutTracker;

  private final ZooKeeperMonitor.TableUserRegistration mUserRegistration;

  private final CountDownLatch mInitializationLatch = new CountDownLatch(1);

  /**
   * Capsule containing all objects which should be mutated in response to a table layout update.
   * The capsule itself is immutable and should be replaced atomically with a new capsule.
   * References only the LayoutCapsule for the most recent layout for this table.
   */
  private volatile LayoutCapsule mLayoutCapsule = null;

  /**
   * Holds the set of LayoutConsumers who should be notified of layout updates.  Held in a weak
   * hash set so that registering to watch a layout does not prevent garbage collection.
   */
  private final Set<LayoutConsumer> mConsumers =
      Collections.synchronizedSet(
          Collections.newSetFromMap(
              new WeakHashMap<LayoutConsumer, Boolean>()));

  /**
   * Create a new table layout monitor for the provided user and table.
   *
   * @param userID of user to register as a table user.
   * @param tableURI of table being registered.
   * @param schemaTable of Kiji table.
   * @param metaTable of Kiji table.
   * @param zkMonitor ZooKeeper connection to register monitor with.
   */
  public TableLayoutMonitor(
      String userID,
      KijiURI tableURI,
      KijiSchemaTable schemaTable,
      KijiMetaTable metaTable,
      ZooKeeperMonitor zkMonitor) {
    mTableURI = tableURI;
    mSchemaTable = schemaTable;
    mMetaTable = metaTable;
    if (zkMonitor == null) {
      mLayoutTracker = null;
      mUserRegistration = null;
    } else {
      mLayoutTracker = zkMonitor.newTableLayoutTracker(mTableURI, new InnerLayoutUpdater());
      mUserRegistration = zkMonitor.newTableUserRegistration(userID, tableURI);
    }
  }

  /**
   * Start this table layout monitor.  Must be called before any other method.
   *
   * @return this table layout monitor.
   * @throws IOException on unrecoverable ZooKeeper error.
   */
  public TableLayoutMonitor start() throws IOException {
    if (mLayoutTracker != null) {
      mLayoutTracker.open();
      try {
        mInitializationLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeInterruptedException(e);
      }
    } else {
      mLayoutCapsule = createCapsule();
    }
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    if (mLayoutTracker != null) {
      mLayoutTracker.close();
    }
    if (mUserRegistration != null) {
      mUserRegistration.close();
    }
  }

  /**
   * Get the LayoutCapsule of a table.
   *
   * @return the table's layout capsule.
   */
  public LayoutCapsule getLayoutCapsule() {
    return mLayoutCapsule;
  }

  /**
   * Register a LayoutConsumer to receive a callback when this table's layout is updated.  The
   * caller *must* hold a strong reference to this TableLayoutMonitor instance until either:
   *
   *  1) the caller unregisters itself using #unregisterConsumer.
   *  2) the caller is garbage collected.
   *
   * Registering as a layout consumer is not guaranteed to keep an object from being garbage
   * collected.
   *
   * @param consumer to notify when the table's layout is updated.
   */
  public void registerLayoutConsumer(LayoutConsumer consumer) {
    mConsumers.add(consumer);
  }

  /**
   * Unregister a LayoutConsumer. This is not necessary in cases where an object wants to continue
   * to receive updates until it is garbage collected.  See the class comment and the
   * #registerConsumer comment for how to properly register as a consumer in this case.
   *
   * @param consumer to unregister.
   */
  public void unregisterLayoutConsumer(LayoutConsumer consumer) {
    mConsumers.remove(consumer);
  }

  /**
   * <p>
   * Get the set of registered layout consumers.  All layout consumers should be updated using
   * {@link LayoutConsumer#update} before this table reports that it has successfully update its
   * layout.
   * </p>
   * <p>
   * This method is for testing purposes only.  It should not be used externally.
   * </p>
   * @return the set of registered layout consumers.
   */
  public Set<LayoutConsumer> getLayoutConsumers() {
    return ImmutableSet.copyOf(mConsumers);
  }

  /**
   * Update all registered LayoutConsumers with a new KijiTableLayout.
   *
   * This method is for testing purposes only.  It should not be used externally.
   *
   * @param layout the new KijiTableLayout with which to update consumers.
   * @throws IOException in case of an error updating LayoutConsumers.
   */
  public void updateLayoutConsumers(KijiTableLayout layout) throws IOException {
    layout.setSchemaTable(mSchemaTable);
    final LayoutCapsule capsule = new LayoutCapsule(layout, new ColumnNameTranslator(layout));
    for (LayoutConsumer consumer : getLayoutConsumers()) {
      consumer.update(capsule);
    }
  }

  /**
   * Reads the most recent layout of this Kiji table from the Kiji instance meta-table.
   *
   * @return the most recent layout of this Kiji table from the Kiji instance meta-table.
   * @throws IOException if unable to retrieve table layout.
   */
  private LayoutCapsule createCapsule() throws IOException {
    final KijiTableLayout layout =
        mMetaTable.getTableLayout(mTableURI.getTable()).setSchemaTable(mSchemaTable);
    return new LayoutCapsule(layout, new ColumnNameTranslator(layout));
  }

  /**
   * Updates the layout of this table in response to a layout update pushed from ZooKeeper.
   */
  private final class InnerLayoutUpdater implements ZooKeeperMonitor.LayoutUpdateHandler {
    /** {@inheritDoc} */
    @Override
    public void update(final byte[] layoutBytes) {
      final String currentLayoutId =
          (mLayoutCapsule == null) ? null : mLayoutCapsule.getLayout().getDesc().getLayoutId();
      final String newLayoutId = Bytes.toString(layoutBytes);
      if (currentLayoutId == null) {
        LOG.info("Setting initial layout for table {} to layout ID {}.",
            mTableURI, newLayoutId);
      } else {
        LOG.info("Updating layout for table {} from layout ID {} to layout ID {}.",
            mTableURI, currentLayoutId, newLayoutId);
      }

      try {
        mLayoutCapsule = createCapsule();
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }

      KijiTableLayout newLayout = mLayoutCapsule.getLayout();

      Preconditions.checkState(
          Objects.equal(newLayout.getDesc().getLayoutId(), newLayoutId),
          "New layout ID %s does not match most recent layout ID %s from meta-table.",
          newLayoutId, newLayout.getDesc().getLayoutId());

      // Propagates the new layout to all consumers:
      for (LayoutConsumer consumer : getLayoutConsumers()) {
        try {
          consumer.update(mLayoutCapsule);
        } catch (IOException ioe) {
          // TODO(SCHEMA-505): Handle exceptions decently
          throw new KijiIOException(ioe);
        }
      }

      // Registers this KijiTable in ZooKeeper as a user of the new table layout,
      // and unregisters as a user of the former table layout.
      try {
        mUserRegistration.updateRegisteredLayout(newLayoutId);
      } catch (IOException ioe) {
        throw new KijiIOException(ioe.getCause());
      }

      mInitializationLatch.countDown();
    }
  }

  /*
    Lifecycle Notes:

    This class has two seperate mechanisms for cleaning up the resources allocated to it:

      1) It implements Closeable.  Call #close, and the resources will be cleaned up, and the
         instance can be safely garbage collected.

      2) It will return a PhantomReference which implements Closeable from #getCloseablePhantomRef.
         This PhantomReference can be used to clean up the resources using a ReferenceQueue and a
         thread which is taking references off the queue and calling close on them.

    Either method is sufficient for cleaning up owned resources.
   */

  /**
   * Retrieve a PhantomReference to this TableLayoutMonitor which implements Closeable and is able
   * to cleanup the resources held by this TableLayoutMonitor.
   *
   * @param queue which will own the PhantomReference.
   * @return a PhantomReference which implements Closeable.
   */
  LayoutMonitorPhantomRef getCloseablePhantomRef(ReferenceQueue<? super TableLayoutMonitor> queue) {
    return new LayoutMonitorPhantomRef(this, queue, mLayoutTracker, mUserRegistration);
  }

  /**
   * A PhantomReference for TableLayoutMonitor which will close the resources held by the
   * TableLayoutMonitor in the case that it gets garbage collected.
   */
  static final class LayoutMonitorPhantomRef extends PhantomReference<TableLayoutMonitor>
      implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(LayoutMonitorPhantomRef.class);

    private final Closeable[] mCloseables;

    private final KijiURI mTableURI;

    private final Set<LayoutConsumer> mConsumers;

    /**
     * Create a new layout monitor phantom reference.
     *
     * @param value TableLayoutMonitor to be cleaned up by phantom reference.
     * @param queue to which the phantom reference should be added.
     * @param closeables resources which should be closed when the phantom reference is queued.
     */
    private LayoutMonitorPhantomRef(
        TableLayoutMonitor value,
        ReferenceQueue<? super TableLayoutMonitor> queue,
        Closeable... closeables) {
      super(value, queue);
      mCloseables = closeables;
      mTableURI = value.mTableURI;
      mConsumers = value.mConsumers;
    }

    /** {@inheritDoc}. */
    @Override
    public void close() {
      LOG.debug("Closing TableLayoutMonitor for table {}.", mTableURI);
      if (!mConsumers.isEmpty()) {
        // This can only happen if registered layout consumers fail to hold a strong reference to
        // the TableLayoutMonitor.
        LOG.error("Closing TableLayoutMonitor with registed layout consumers: {}", mConsumers);
      }
      for (Closeable closeable : mCloseables) {
        try {
          closeable.close();
        } catch (IOException ioe) {
          LOG.warn("Unable to close resource:", ioe);
        }
      }
    }
  }
}
