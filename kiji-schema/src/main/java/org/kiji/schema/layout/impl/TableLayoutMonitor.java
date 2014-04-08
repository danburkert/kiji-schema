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

import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.kiji.schema.layout.KijiColumnNameTranslator;
import org.kiji.schema.util.AutoCloseable;
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
public class TableLayoutMonitor implements AutoCloseable {

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
  private final AtomicReference<LayoutCapsule> mLayoutCapsule = new AtomicReference<LayoutCapsule>();

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
      mUserRegistration = zkMonitor.newTableUserRegistration(userID, tableURI);
      mLayoutTracker = zkMonitor.newTableLayoutTracker(
          mTableURI,
          new InnerLayoutUpdater(
              mUserRegistration,
              mInitializationLatch,
              mLayoutCapsule,
              mTableURI,
              mConsumers,
              mMetaTable,
              mSchemaTable));
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
      final KijiTableLayout layout =
          mMetaTable.getTableLayout(mTableURI.getTable()).setSchemaTable(mSchemaTable);
      mLayoutCapsule.set(new LayoutCapsule(layout, KijiColumnNameTranslator.from(layout)));
    }
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public CloseablePhantomRef getCloseablePhantomRef(ReferenceQueue<AutoCloseable> queue) {
    return new CloseablePhantomRef(this, queue, mUserRegistration, mLayoutTracker);
  }

  /**
   * Get the LayoutCapsule of a table.
   *
   * @return the table's layout capsule.
   */
  public LayoutCapsule getLayoutCapsule() {
    return mLayoutCapsule.get();
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
    final LayoutCapsule capsule = new LayoutCapsule(layout, KijiColumnNameTranslator.from(layout));
    for (LayoutConsumer consumer : getLayoutConsumers()) {
      consumer.update(capsule);
    }
  }

  /**
   * Updates the layout of this table in response to a layout update pushed from ZooKeeper.
   */
  private static final class InnerLayoutUpdater implements ZooKeeperMonitor.LayoutUpdateHandler {

    private final ZooKeeperMonitor.TableUserRegistration mUserRegistration;

    private final CountDownLatch mInitializationLatch;

    private final AtomicReference<LayoutCapsule> mLayoutCapsule;

    private final KijiURI mTableURI;

    private final Set<LayoutConsumer> mConsumers;

    private final KijiMetaTable mMetaTable;

    private final KijiSchemaTable mSchemaTable;

    private InnerLayoutUpdater(
        ZooKeeperMonitor.TableUserRegistration userRegistration,
        CountDownLatch initializationLatch,
        AtomicReference<LayoutCapsule> layoutCapsule,
        KijiURI tableURI,
        Set<LayoutConsumer> consumers,
        KijiMetaTable metaTable,
        KijiSchemaTable schemaTable
    ) {
      mUserRegistration = userRegistration;
      mInitializationLatch = initializationLatch;
      mLayoutCapsule = layoutCapsule;
      mTableURI = tableURI;
      mConsumers = consumers;
      mMetaTable = metaTable;
      mSchemaTable = schemaTable;
    }


    /** {@inheritDoc} */
    @Override
    public void update(final byte[] layoutBytes) {
      final String currentLayoutId =
          (mLayoutCapsule.get() == null)
              ? null
              : mLayoutCapsule.get().getLayout().getDesc().getLayoutId();
      final String newLayoutId = Bytes.toString(layoutBytes);
      if (currentLayoutId == null) {
        LOG.info("Setting initial layout for table {} to layout ID {}.",
            mTableURI, newLayoutId);
      } else {
        LOG.info("Updating layout for table {} from layout ID {} to layout ID {}.",
            mTableURI, currentLayoutId, newLayoutId);
      }

      try {
        final KijiTableLayout layout =
            mMetaTable.getTableLayout(mTableURI.getTable()).setSchemaTable(mSchemaTable);
        mLayoutCapsule.set(new LayoutCapsule(layout, KijiColumnNameTranslator.from(layout)));
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }

      KijiTableLayout newLayout = mLayoutCapsule.get().getLayout();

      Preconditions.checkState(
          Objects.equal(newLayout.getDesc().getLayoutId(), newLayoutId),
          "New layout ID %s does not match most recent layout ID %s from meta-table.",
          newLayoutId, newLayout.getDesc().getLayoutId());

      // Propagates the new layout to all consumers:
      for (LayoutConsumer consumer : ImmutableSet.copyOf(mConsumers)) {
        try {
          consumer.update(mLayoutCapsule.get());
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
}
