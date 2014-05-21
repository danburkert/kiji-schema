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
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.impl.LayoutConsumer;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.util.AutoReferenceCounted;
import org.kiji.schema.util.JvmId;
import org.kiji.schema.zookeeper.TableLayoutTracker;
import org.kiji.schema.zookeeper.TableLayoutUpdateHandler;
import org.kiji.schema.zookeeper.TableUserRegistration;

/**
 * TableLayoutMonitor provides three services for users of table layouts:
 *
 *  1) it acts as a KijiTableLayout cache which is automatically refreshed when the table layout is
 *     updated.
 *  2) it allows LayoutConsumer instances to register to receive a callback when the table layout
 *     changes.
 *  3) it registers as a table user in ZooKeeper, and keeps that registration up-to-date with the
 *     oldest version of table layout being used by registered LayoutConsumers.
 *
 * This class is thread-safe with the following caveat:
 *  * LayoutConsumer instances who register to receive callbacks *must* keep a strong reference to
 *    this TableLayoutMonitor for as long as the registration should remain active. This is either
 *    for the life of the layout consumer if it is never unregistered, or until
 *    {@link #unregisterLayoutConsumer(LayoutConsumer)} is called.
 *
 * @param <T> specific type of {@link LayoutCapsule} cached by this table layout monitor.
 */
@ApiAudience.Private
public final class TableLayoutMonitor<T extends LayoutCapsule<?>> implements AutoReferenceCounted {

  private static final Logger LOG = LoggerFactory.getLogger(TableLayoutMonitor.class);

  private static final AtomicLong TABLE_COUNTER = new AtomicLong(0);

  private final KijiURI mTableURI;

  private final KijiSchemaTable mSchemaTable;

  private final KijiMetaTable mMetaTable;

  private final TableLayoutTracker mTableLayoutTracker;

  private final TableUserRegistration mUserRegistration;

  private final CountDownLatch mInitializationLatch;

  private final Function<KijiTableLayout, T> mLayoutCapsuleFactory;

  /** States of a table layout monitor. */
  private static enum State {
    /** The table layout monitor has been created, but not yet started. */
    INITIALIZED,

    /** This instance monitor is started, and is currently monitoring the table. */
    STARTED
  }

  private final AtomicReference<State> mState = new AtomicReference<State>();

  /**
   * Capsule containing all objects which should be mutated in response to a table layout update.
   * The capsule itself is immutable and should be replaced atomically with a new capsule.
   * References only the {@link LayoutCapsule} for the most recent layout for this table.
   */
  private final AtomicReference<T> mLayoutCapsule = new AtomicReference<T>();

  /**
   * Holds the set of LayoutConsumers who should be notified of layout updates.  Held in a weak
   * hash set so that registering to watch a layout does not prevent garbage collection.
   */
  private final Set<LayoutConsumer<T>> mConsumers =
      Collections.newSetFromMap(new MapMaker().weakKeys().<LayoutConsumer<T>, Boolean>makeMap());

  /**
   * Create a new table layout monitor for the provided user and table.
   *
   * @param tableURI of table being registered.
   * @param layoutCapsuleFactory function to create concrete instances of {@link LayoutCapsule}.
   * @param schemaTable of Kiji table.
   * @param metaTable of Kiji table.
   * @param zkClient ZooKeeper connection to register monitor with, or null if ZooKeeper is
   *        unavailable (SYSTEM_1_0).
   */
  public TableLayoutMonitor(
      KijiURI tableURI,
      Function<KijiTableLayout, T> layoutCapsuleFactory,
      KijiSchemaTable schemaTable,
      KijiMetaTable metaTable,
      CuratorFramework zkClient) {
    mTableURI = tableURI;
    mSchemaTable = schemaTable;
    mLayoutCapsuleFactory = layoutCapsuleFactory;
    mMetaTable = metaTable;
    if (zkClient == null) {
      mTableLayoutTracker = null;
      mUserRegistration = null;
      mInitializationLatch = null;
    } else {
      mUserRegistration = new TableUserRegistration(zkClient, tableURI, generateTableUserID());
      mInitializationLatch = new CountDownLatch(1);
      mTableLayoutTracker = new TableLayoutTracker(
          zkClient,
          mTableURI,
          new InnerLayoutUpdater<T>(
              mUserRegistration,
              mInitializationLatch,
              mLayoutCapsule,
              mLayoutCapsuleFactory,
              mTableURI,
              mConsumers,
              mMetaTable,
              mSchemaTable));
    }
    mState.compareAndSet(null, State.INITIALIZED);
  }

  /**
   * Start this table layout monitor.  Must be called before any other method.
   *
   * @return this table layout monitor.
   * @throws IOException on unrecoverable ZooKeeper or meta table error.
   */
  public TableLayoutMonitor<T> start() throws IOException {
    Preconditions.checkState(
        mState.compareAndSet(State.INITIALIZED, State.STARTED),
        "Cannot start TableLayoutMonitor in state %s.", mState.get());
    if (mTableLayoutTracker != null) {
      mTableLayoutTracker.start();
      try {
        mInitializationLatch.await();
      } catch (InterruptedException e) {
        throw new RuntimeInterruptedException(e);
      }
    } else {
      final KijiTableLayout layout =
          mMetaTable.getTableLayout(mTableURI.getTable()).setSchemaTable(mSchemaTable);
      mLayoutCapsule.set(mLayoutCapsuleFactory.apply(layout));
    }
    return this;
  }

  /** {@inheritDoc} */
  @Override
  public Collection<Closeable> getCloseableResources() {
    Preconditions.checkState(mState.get() == State.STARTED,
        "TableLayoutMonitor has not been started.");
    ImmutableList.Builder<Closeable> resources = ImmutableList.builder();
    if (mUserRegistration != null) {
      resources.add(mUserRegistration);
    }
    if (mTableLayoutTracker != null) {
      resources.add(mTableLayoutTracker);
    }
    return resources.build();
  }

  /**
   * Get the LayoutCapsule of a table.
   *
   * @return the table's layout capsule.
   */
  public T getLayoutCapsule() {
    Preconditions.checkState(mState.get() == State.STARTED,
        "TableLayoutMonitor has not been started.");
    return mLayoutCapsule.get();
  }

  /**
   * Register a LayoutConsumer to receive a callback when this table's layout is updated.  The
   * caller *must* hold a strong reference to this TableLayoutMonitor instance until either:
   *
   *  1) the caller unregisters itself using {@link #unregisterLayoutConsumer(LayoutConsumer)}.
   *  2) the caller is garbage collected.
   *
   * Registering as a layout consumer is not guaranteed to keep an object from being garbage
   * collected.
   *
   * The consumer will immediately be notified of the current layout before returning.
   *
   * @param consumer to notify when the table's layout is updated.
   * @throws java.io.IOException if the consumer's {@code #update} method throws IOException.
   */
  public void registerLayoutConsumer(LayoutConsumer<T> consumer) throws IOException {
    Preconditions.checkState(mState.get() == State.STARTED,
        "TableLayoutMonitor has not been started.");
    mConsumers.add(consumer);
    consumer.update(getLayoutCapsule());
  }

  /**
   * Unregister a LayoutConsumer. This is not necessary in cases where a consumer wants to continue
   * to receive updates until it is garbage collected.  See the class comment and the
   * {@link #registerLayoutConsumer(LayoutConsumer)} comment for how to properly register as a
   * consumer in this case.
   *
   * @param consumer to unregister.
   */
  public void unregisterLayoutConsumer(LayoutConsumer consumer) {
    Preconditions.checkState(mState.get() == State.STARTED,
        "TableLayoutMonitor has not been started.");
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
  public Set<LayoutConsumer<T>> getLayoutConsumers() {
    Preconditions.checkState(mState.get() == State.STARTED,
        "TableLayoutMonitor has not been started.");
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
    Preconditions.checkState(mState.get() == State.STARTED,
        "TableLayoutMonitor has not been started.");
    layout.setSchemaTable(mSchemaTable);
    final T capsule = mLayoutCapsuleFactory.apply(layout);
    for (LayoutConsumer<T> consumer : getLayoutConsumers()) {
      consumer.update(capsule);
    }
  }

  /**
   * Generates a uniquely identifying ID for a table user.
   *
   * @return a uniquely identifying ID for a table user.
   */
  private static String generateTableUserID() {
    return String.format("%s;HBaseKijiTable@%s", JvmId.get(), TABLE_COUNTER.getAndIncrement());
  }

  /**
   * Updates the layout of this table in response to a layout update pushed from ZooKeeper.
   */
  private static final class InnerLayoutUpdater<T extends LayoutCapsule<?>>
      implements TableLayoutUpdateHandler {

    private final TableUserRegistration mUserRegistration;

    private final CountDownLatch mInitializationLatch;

    private final AtomicReference<T> mLayoutCapsule;

    private final KijiURI mTableURI;

    private final Set<LayoutConsumer<T>> mConsumers;

    private final KijiMetaTable mMetaTable;

    private final KijiSchemaTable mSchemaTable;

    private final Function<KijiTableLayout, T> mLayoutCapsuleFactory;

    // CSOFF: ParameterNumberCheck
    /**
     * Create an InnerLayoutUpdater to update the layout of this table in response to a layout node
     * change in ZooKeeper.
     *
     * @param userRegistration ZooKeeper table user registration.
     * @param initializationLatch latch that will be counted down upon successful initialization.
     * @param layoutCapsule layout capsule to store most recent layout in.
     * @param layoutCapsuleFactory function to create concrete instances of {@link LayoutCapsule}.
     * @param tableURI URI of table whose layout is to be tracked.
     * @param consumers Set of layout consumers to notify on table layout update.
     * @param metaTable containing meta information.
     * @param schemaTable containing schema information.
     */
    private InnerLayoutUpdater(
        TableUserRegistration userRegistration,
        CountDownLatch initializationLatch,
        AtomicReference<T> layoutCapsule,
        Function<KijiTableLayout, T> layoutCapsuleFactory,
        KijiURI tableURI,
        Set<LayoutConsumer<T>> consumers,
        KijiMetaTable metaTable,
        KijiSchemaTable schemaTable
    ) {
      mUserRegistration = userRegistration;
      mInitializationLatch = initializationLatch;
      mLayoutCapsule = layoutCapsule;
      mLayoutCapsuleFactory = layoutCapsuleFactory;
      mTableURI = tableURI;
      mConsumers = consumers;
      mMetaTable = metaTable;
      mSchemaTable = schemaTable;
    }
    // CSON: ParameterNumberCheck

    /** {@inheritDoc} */
    @Override
    public void update(final String notifiedLayoutID) {
      final String currentLayoutId =
          (mLayoutCapsule.get() == null)
              ? null
              : mLayoutCapsule.get().getLayout().getDesc().getLayoutId();
      if (currentLayoutId == null) {
        LOG.debug("Setting initial layout for table {} to layout ID {}.",
            mTableURI, notifiedLayoutID);
      } else {
        LOG.debug("Updating layout for table {} from layout ID {} to layout ID {}.",
            mTableURI, currentLayoutId, notifiedLayoutID);
      }

      try {
        final KijiTableLayout newLayout =
            mMetaTable.getTableLayout(mTableURI.getTable()).setSchemaTable(mSchemaTable);

        Preconditions.checkState(
            Objects.equal(newLayout.getDesc().getLayoutId(), notifiedLayoutID),
            "New layout ID %s does not match most recent layout ID %s from meta-table.",
            notifiedLayoutID, newLayout.getDesc().getLayoutId());

        mLayoutCapsule.set(mLayoutCapsuleFactory.apply(newLayout));

        // Propagates the new layout to all consumers.
        // A copy of mConsumers is made in order to avoid concurrent modification issues. The
        // contract of Guava's ImmutableSet specifies that #copyOf is safe on concurrent collections
        for (LayoutConsumer<T> consumer : ImmutableSet.copyOf(mConsumers)) {
          consumer.update(mLayoutCapsule.get());
        }

        // Registers this KijiTable in ZooKeeper as a user of the new table layout,
        // and unregisters as a user of the former table layout.
        if (currentLayoutId == null) {
          mUserRegistration.start(notifiedLayoutID);
        } else {
          mUserRegistration.updateLayoutID(notifiedLayoutID);
        }

        mInitializationLatch.countDown();
      } catch (IOException e) {
        throw new KijiIOException(e);
      }
    }
  }
}
