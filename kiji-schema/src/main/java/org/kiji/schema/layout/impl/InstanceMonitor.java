package org.kiji.schema.layout.impl;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.curator.framework.CuratorFramework;
import org.kiji.schema.KijiIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.InstanceUserRegistrationDesc;
import org.kiji.schema.util.ProtocolVersion;
import org.kiji.schema.layout.impl.TableLayoutMonitor.LayoutMonitorPhantomRef;

public class InstanceMonitor implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(InstanceMonitor.class);

  private volatile boolean mIsClosed = false;

  private final ReferenceQueue<TableLayoutMonitor> mRefQueue =
      new ReferenceQueue<TableLayoutMonitor>();

  private final Set<LayoutMonitorPhantomRef> mReferences =
      Collections.newSetFromMap(new ConcurrentHashMap<LayoutMonitorPhantomRef, Boolean>());

  private final ExecutorService mExecutor = Executors.newCachedThreadPool();

  private final String mUserID;

  private final ProtocolVersion mSystemVersion;

  /** URI of Kiji instance which own the tables whose layouts to cache. */
  private final KijiURI mInstanceURI;

  /** Meta Table which holds table layout information. */
  private final KijiMetaTable mMetaTable;

  private final KijiSchemaTable mSchemaTable;

  /** Connection to ZooKeeper cluster. */
  private final CuratorFramework mZKClient;

  /** Cache holding a ZooKeeper node for each table. */
  private final LoadingCache<String, TableLayoutMonitor> mTableLayoutMonitors;

  private final ZooKeeperMonitor.InstanceUserRegistration mUserRegistration;

  public InstanceMonitor(
      String userID,
      ProtocolVersion systemVersion,
      KijiURI instanceURI,
      KijiMetaTable metaTable,
      KijiSchemaTable schemaTable,
      CuratorFramework zkClient) {

    mInstanceURI = instanceURI;
    mMetaTable = metaTable;
    mSchemaTable = schemaTable;
    mZKClient = zkClient;
    mUserID = userID;
    mSystemVersion = systemVersion;

    mTableLayoutMonitors =  CacheBuilder
        .newBuilder()
        // Only keep a weak reference to cached TableLayoutMonitors.  This allows them to be
        // collected when they don't have any remaining clients.
        .weakValues()
        .removalListener(new TableLayoutMonitorRemovalListener())
        .build(new TableLayoutMonitorCacheLoader());

    mUserRegistration = ZooKeeperMonitor.newInstanceUserRegistration(zkClient, mInstanceURI);
  }

  public TableLayoutMonitor getTableLayoutMonitor(String tableName) throws IOException {
    Preconditions.checkState(!mIsClosed, "InstanceMonitor is closed.");
    try {
      return mTableLayoutMonitors.get(tableName);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new KijiIOException(cause);
      }
    } catch (UncheckedExecutionException ue) {
      throw new KijiIOException(ue.getCause());
    }
  }

  public InstanceMonitor start() throws IOException {
    Preconditions.checkState(!mIsClosed, "InstanceMonitor is closed.");
    InstanceUserRegistrationDesc registration = InstanceUserRegistrationDesc
        .newBuilder()
        .setSystemVersion(mSystemVersion.toString())
        .setUserId(mUserID)
        .build();
    mUserRegistration.start(registration);
    mExecutor.execute(new PhantomRefCloser(mRefQueue, mReferences));
    return this;
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing InstanceMonitor for instance {}.", mInstanceURI);
    mIsClosed = true;
    mUserRegistration.close();
    mTableLayoutMonitors.invalidateAll();
    mExecutor.shutdown();
  }

  /**
   * KijiTableLayout CacheLoader that adds a ColumnNameTranslator to a cache in the same step.
   */
  private class TableLayoutMonitorCacheLoader extends CacheLoader<String, TableLayoutMonitor> {
    @Override
    public TableLayoutMonitor load(String tableName) throws IOException {
      KijiURI tableURI = KijiURI.newBuilder(mInstanceURI).withTableName(tableName).build();

      // TODO: change to debug
      LOG.info("Creating TableLayoutMonitor for table {}.", tableURI);

      TableLayoutMonitor monitor =
          new TableLayoutMonitor(mUserID, tableURI, mMetaTable, mSchemaTable, mZKClient).start();

       LayoutMonitorPhantomRef reference = monitor.getCloseablePhantomRef(mRefQueue);

      mReferences.add(reference);
      return monitor;
    }
  }

  private static class TableLayoutMonitorRemovalListener
      implements RemovalListener<String, TableLayoutMonitor> {
    @Override
    public void onRemoval(RemovalNotification<String, TableLayoutMonitor> notification) {
      TableLayoutMonitor monitor = notification.getValue();
      if (monitor != null) {
        // Cleanup the TableLayoutMonitor if it hasn't been collected
        // TODO: change to debug
        LOG.info("Cleaning up TableLayoutMonitor for table {}.", notification.getKey());
        try {
          monitor.close();
        } catch(IOException ioe) {
          LOG.warn("Unable to cleanup TableLayoutMonitor for table {}.", notification.getKey());
        }
      }
    }
  }

  private static final class PhantomRefCloser implements Runnable {
    private final ReferenceQueue<TableLayoutMonitor> mRefQueue;
    private final Set<LayoutMonitorPhantomRef> mReferences;

    private PhantomRefCloser(
        ReferenceQueue<TableLayoutMonitor> refQueue,
        Set<LayoutMonitorPhantomRef> references) {
      mRefQueue = refQueue;
      mReferences = references;
    }

    @Override
    public void run() {
      while (true) {
        try {
          LayoutMonitorPhantomRef ref = (LayoutMonitorPhantomRef) mRefQueue.remove();
          ref.close();
          mReferences.remove(ref);
        } catch (Exception e) {
          LOG.warn("Exception while closing TableLayoutMonitor from a phantom reference:", e);
        }
      }
    }
  }
}
