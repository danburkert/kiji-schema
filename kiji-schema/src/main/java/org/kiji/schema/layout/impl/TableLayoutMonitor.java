package org.kiji.schema.layout.impl;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.CuratorFramework;
import org.kiji.schema.KijiIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableUserRegistrationDesc;
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
 * This class is thread-safe with one caveat:
 *  1) LayoutConsumer instances who register to receive callbacks *must* keep a strong reference to
 *     this TableLayoutMonitor for as long as the registration should remain active. This is either
 *     for the life of the object if it is never unregistered, or until #unregisterConsumer
 *     is called.
 */
public class TableLayoutMonitor implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(TableLayoutMonitor.class);

  private final String mUserID;

  private final KijiURI mTableURI;

  private final KijiMetaTable mMetaTable;

  private final KijiSchemaTable mSchemaTable;

  private final ZooKeeperMonitor.TableUserRegistration mTableRegistration;

  private final ZooKeeperMonitor.LayoutTracker mTracker;

  private volatile boolean mIsClosed;

  /**
   * Holds the current layout capsule for the table. There is no need to protect against concurrent
   * updates since the table layout is protected by a global synchronous ZK lock.
   */
  private volatile LayoutCapsule mCapsule;

  /**
   * Holds the set of LayoutConsumers who should be notified of layout updates.  Held in a weak
   * hash set so that registering to watch a layout does not prevent garbage collection.
   */
  private final Set<LayoutConsumer> mConsumers =
      Collections.synchronizedSet(
          Collections.newSetFromMap(
              new WeakHashMap<LayoutConsumer, Boolean>()));

  public TableLayoutMonitor(
      final String userId,
      final KijiURI tableURI,
      final KijiMetaTable metaTable,
      final KijiSchemaTable schemaTable,
      final CuratorFramework zkClient) {

    mUserID = userId;
    mTableURI = tableURI;
    mMetaTable = metaTable;
    mSchemaTable = schemaTable;

    mTracker = ZooKeeperMonitor.newTableLayoutTracker(
        zkClient,
        tableURI,
        new LayoutChangeListener());

    mTableRegistration = ZooKeeperMonitor.newTableUserRegistration(zkClient, mTableURI);

    mIsClosed = false;
  }

  private TableUserRegistrationDesc createRegistration() throws IOException {
    return TableUserRegistrationDesc
        .newBuilder()
        .setLayoutId(mTracker.getLayoutID())
        .setUserId(mUserID)
        .build();
  }

  /**
   * Reads the most recent layout of this Kiji table from the Kiji instance meta-table.
   *
   * @return the most recent layout of this Kiji table from the Kiji instance meta-table.
   */
  private LayoutCapsule createCapsule() throws IOException {
    final KijiTableLayout layout =
        mMetaTable.getTableLayout(mTableURI.getTable()).setSchemaTable(mSchemaTable);
    return new LayoutCapsule(layout, new ColumnNameTranslator(layout));
  }

  /**
   * Start this table layout monitor.
   *
   * @throws IOException if starting fails.
   */
  public TableLayoutMonitor start() throws IOException {
    Preconditions.checkState(!mIsClosed, "TableLayoutMonitor is closed.");
    mCapsule = createCapsule();
    mTracker.start();
    mTableRegistration.start(createRegistration());
    return this;
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
  public void registerConsumer(LayoutConsumer consumer) throws IOException {
    Preconditions.checkState(!mIsClosed, "TableLayoutMonitor is closed.");
    mConsumers.add(consumer);
    consumer.update(getLayoutCapsule());
  }

  /**
   * Unregister a LayoutConsumer. This is not necessary in cases where an object wants to continue
   * to receive updates until it is garbage collected.  See the class comment and the
   * #registerConsumer comment for how to properly register as a consumer in this case.
   *
   * @param consumer to unregister.
   */
  public void unregisterConsumer(LayoutConsumer consumer) {
    Preconditions.checkState(!mIsClosed, "TableLayoutMonitor is closed.");
    mConsumers.remove(consumer);
  }

  public Set<LayoutConsumer> getLayoutConsumers() {
    Preconditions.checkState(!mIsClosed, "TableLayoutMonitor is closed.");
    return ImmutableSet.copyOf(mConsumers);
  }

  /**
   * Get the KijiTableLayout of a table.
   *
   * @return the KijiTableLayout of the table.
   */
  public KijiTableLayout getTableLayout() {
    return getLayoutCapsule().getLayout();
  }

  /**
   * Get the ColumnNameTranslator of a table.
   *
   * @return the ColumnNameTranslator of the table.
   */
  public ColumnNameTranslator getColumnNameTranslator() {
    return getLayoutCapsule().getColumnNameTranslator();
  }

  /**
   * Get the LayoutCapsule of a table.
   *
   * @return the table's layout capsule.
   */
  public LayoutCapsule getLayoutCapsule() {
    Preconditions.checkState(!mIsClosed, "TableLayoutMonitor is closed.");
    return mCapsule;
  }

  private final class LayoutChangeListener implements ZooKeeperMonitor.LayoutUpdateHandler {
    @Override
    public void update(String newLayoutID) {
      // This handler does not have to protect against concurrent layout updates, because layout
      // updates are protected by an exclusive global ZK lock.

      String currentLayoutID = mCapsule.getLayout().getDesc().getLayoutId();
      if (newLayoutID == null) {
        // TODO: figure out what to do here.
        LOG.info("Layout node deleted.");
      } else if (currentLayoutID == null) {
        LOG.info("Setting initial layout ID for table {} to {}.", mTableURI, newLayoutID);
      } else {
        LOG.info("Updating layout ID for table {} from {} to {}.",
            mTableURI, currentLayoutID, newLayoutID);
      }

      // TODO(SCHEMA-503): the meta-table doesn't provide a way to look-up a layout by ID:
      try {
        mCapsule = createCapsule();
      } catch (IOException ioe) {
        throw new KijiIOException(ioe);
      }

      if (!mCapsule.getLayout().getDesc().getLayoutId().equals(newLayoutID)) {
        LOG.warn("New layout ID {} does not match most recent layout ID {} from meta-table.",
            newLayoutID, mCapsule.getLayout().getDesc().getLayoutId());
      }

      // Make copy of current consumers that is safe to iterate without synchronizing. It is
      // required that this come after the capsule is updated so that all registrations which
      // happened before the new capsule was set are notified.  There is a small window here that
      // consumers may get notified unnecessarily, but that is OK.
      Set<LayoutConsumer> consumers = ImmutableSet.copyOf(mConsumers);

      // TODO: make below non-blocking
      for (LayoutConsumer consumer : consumers) {
        try {
          consumer.update(mCapsule);
        } catch (IOException e) {
          // TODO(SCHEMA-505): Handle exceptions decently
          LOG.warn(String.format("Unable to update layout consumer %s.", consumer), e);
        }
      }

      // Notify watchers in ZK that all table users have updated to the new layout.
      try {
        mTableRegistration.update(createRegistration());
      } catch (Exception e) {
        throw new KijiIOException(e);
      }
    }
  }

  /**
   * Holds a cached KijiTableLayout and ColumnNameTranslator instance for a table.
   *
   * Instances of this class are thread-safe.
   */
  public final class LayoutCapsule {
    private final KijiTableLayout mLayout;
    private final ColumnNameTranslator mTranslator;

    private LayoutCapsule(KijiTableLayout layout, ColumnNameTranslator translator) {
      mLayout = layout;
      mTranslator = translator;
    }

    /**
     * Retrieve the Kiji table layout from this capsule.
     *
     * @return the Kiji table layout.
     */
    public KijiTableLayout getLayout() {
      return mLayout;
    }

    /**
     * Retrive the column name translator from this capsule.
     *
     * @return the column name translator.
     */
    public ColumnNameTranslator getColumnNameTranslator() {
      return mTranslator;
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
   * Close this TableLayoutMonitor.
   *
   * @throws IOException on unrecoverable ZooKeeper error.
   */
  @Override
  public void close() throws IOException {
    mIsClosed = true;
    // Control flow:
    // 1. Ensure that it is valid to close the table layout monitor at this time.
    // 2. Regardless of validity, attempt to close mTracker.
    // 3. Regardless of success, attempt to close mTableRegistration.
    try {
      Preconditions.checkState(mConsumers.isEmpty(),
          "close() called while there are registered consumers: %s.", mConsumers);
    } finally {
      try {
        mTracker.close();
      } finally {
        mTableRegistration.close();
      }
    }
  }

  /**
   * Retrieve a PhantomReference to this TableLayoutMonitor which implements Closeable and is able
   * to cleanup the resources held by this TableLayoutMonitor.
   *
   * @param queue which will own the PhantomReference.
   * @return a PhantomReference which implements Closeable.
   */
  LayoutMonitorPhantomRef getCloseablePhantomRef(
      ReferenceQueue<? super TableLayoutMonitor> queue
  ) {
    return new LayoutMonitorPhantomRef(this, queue, mTracker, mTableRegistration);
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

    private LayoutMonitorPhantomRef(
        TableLayoutMonitor value,
        ReferenceQueue<? super TableLayoutMonitor> queue,
        Closeable... closeables) {
      super(value, queue);
      mCloseables = closeables;
      mTableURI = value.mTableURI;
      mConsumers = value.mConsumers;
    }

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
