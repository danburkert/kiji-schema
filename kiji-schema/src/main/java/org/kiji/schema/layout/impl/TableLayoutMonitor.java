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
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.kiji.schema.RuntimeInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.LayoutConsumer;

public class TableLayoutMonitor implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TableLayoutMonitor.class);

  private final String mUserID;

  private final KijiURI mTableURI;

  private final Kiji mKiji;

  private final ZooKeeperMonitor mZKMonitor;

  private final ZooKeeperMonitor.LayoutTracker mLayoutTracker;

  private final CountDownLatch initializationLatch = new CountDownLatch(1);

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

  public TableLayoutMonitor(
      String userID,
      KijiURI tableURI,
      Kiji kiji,
      ZooKeeperMonitor zkMonitor) {
    mUserID = userID;
    mTableURI = tableURI;
    mKiji = kiji;
    mZKMonitor = zkMonitor;
    if (zkMonitor == null) {
      mLayoutTracker = null;
    } else {
      mLayoutTracker = mZKMonitor.newTableLayoutTracker(mTableURI, new InnerLayoutUpdater());
    }
  }

  public TableLayoutMonitor start() throws IOException {
    if (mLayoutTracker != null) {
      mLayoutTracker.open();
      try {
        initializationLatch.await();
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
    layout.setSchemaTable(mKiji.getSchemaTable());
    final LayoutCapsule capsule = new LayoutCapsule(layout, new ColumnNameTranslator(layout));
    for (LayoutConsumer consumer : getLayoutConsumers()) {
      consumer.update(capsule);
    }
  }

  /**
   * Reads the most recent layout of this Kiji table from the Kiji instance meta-table.
   *
   * @return the most recent layout of this Kiji table from the Kiji instance meta-table.
   */
  private LayoutCapsule createCapsule() throws IOException {
    final KijiTableLayout layout =
        mKiji
            .getMetaTable()
            .getTableLayout(mTableURI.getTable())
            .setSchemaTable(mKiji.getSchemaTable());
    return new LayoutCapsule(layout, new ColumnNameTranslator(layout));
  }

  /** Updates the layout of this table in response to a layout update pushed from ZooKeeper. */
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
        mZKMonitor.registerTableUser(mTableURI, mUserID, newLayoutId);
        if (currentLayoutId != null) {
          mZKMonitor.unregisterTableUser(mTableURI, mUserID, currentLayoutId);
        }
      } catch (KeeperException ke) {
        // TODO(SCHEMA-505): Handle exceptions decently
        throw new KijiIOException(ke);
      }

      initializationLatch.countDown();
    }
  }
}
