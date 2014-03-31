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
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.kiji.schema.scratch.CloseablePhantomRefCloser;
import org.kiji.schema.util.ResourceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiSchemaTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ProtocolVersion;

/**
 * A Kiji instance monitors. Registers a client as an instance user in ZooKeeper, and provides
 * table layout monitors.
 */
public class InstanceMonitor implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(InstanceMonitor.class);

  private volatile boolean mIsClosed = false;

  private final ReferenceQueue<TableLayoutMonitor> mRefQueue =
      new ReferenceQueue<TableLayoutMonitor>();

  private final CloseablePhantomRefCloser mCloser = new CloseablePhantomRefCloser();

  private final String mUserID;

  private final KijiURI mInstanceURI;

  private final KijiSchemaTable mSchemaTable;

  private final KijiMetaTable mMetaTable;

  private final ZooKeeperMonitor mZKMonitor;

  private final LoadingCache<String, TableLayoutMonitor> mTableLayoutMonitors;

  private final ZooKeeperMonitor.InstanceUserRegistration mUserRegistration;

  /**
   * Create an instance monitor for a Kiji instance.
   *
   * @param userID of instance user.
   * @param systemVersion of instance user.
   * @param instanceURI uri of instance to monitor.
   * @param schemaTable of instance.
   * @param metaTable of instance.
   * @param zkMonitor ZooKeeper connection to use for monitoring.
   */
  public InstanceMonitor(
      String userID,
      ProtocolVersion systemVersion,
      KijiURI instanceURI,
      KijiSchemaTable schemaTable,
      KijiMetaTable metaTable,
      ZooKeeperMonitor zkMonitor) {

    mUserID = userID;
    mInstanceURI = instanceURI;
    mSchemaTable = schemaTable;
    mMetaTable = metaTable;
    mZKMonitor = zkMonitor;

    mTableLayoutMonitors =  CacheBuilder
        .newBuilder()
        // Only keep a weak reference to cached TableLayoutMonitors.  This allows them to be
        // collected when they don't have any remaining clients.
        .weakValues()
        .removalListener(new TableLayoutMonitorRemovalListener())
        .build(new TableLayoutMonitorCacheLoader());

    if (zkMonitor != null) {
      mUserRegistration = zkMonitor.newInstanceUserRegistration(
          userID, systemVersion.toCanonicalString(), instanceURI);
    } else {
      mUserRegistration = null;
    }
  }

  /**
   * Get a table layout monitor for the provided table.  Table layout monitors are cached for as
   * long as they are in use.
   *
   * @param tableName of table layout monitor to retrieve.
   * @return a table layout monitor for the table.
   * @throws IOException if unrecoverable ZooKeeper exception.
   */
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

  /**
   * Start this InstanceMonitor.  Must be called before any other method is valid.
   *
   * @return this.
   * @throws IOException on unrecoverable ZooKeeper exception.
   */
  public InstanceMonitor start() throws IOException {
    Preconditions.checkState(!mIsClosed, "InstanceMonitor is closed.");
    if (mUserRegistration != null) {
      mUserRegistration.start();
    }
    return this;
  }

  /**
   * Close this InstanceMonitor.  Should be called when this instance monitor is no longer needed.
   * All table monitor objects owned by this instance monitor will also be closed.
   *
   * @throws IOException on unrecoverable ZooKeeper exception.
   */
  @Override
  public void close() throws IOException {
    LOG.debug("Closing InstanceMonitor for instance {}.", mInstanceURI);
    mIsClosed = true;

    if (mUserRegistration != null) {
      mUserRegistration.close();
    }

    mTableLayoutMonitors.invalidateAll();
    mCloser.close();
  }

  /**
   * KijiTableLayout CacheLoader that adds a ColumnNameTranslator to a cache in the same step.
   */
  private class TableLayoutMonitorCacheLoader extends CacheLoader<String, TableLayoutMonitor> {
    @Override
    public TableLayoutMonitor load(String tableName) throws IOException {
      KijiURI tableURI = KijiURI.newBuilder(mInstanceURI).withTableName(tableName).build();

      LOG.debug("Creating TableLayoutMonitor for table {}.", tableURI);

      TableLayoutMonitor monitor =
          new TableLayoutMonitor(mUserID, tableURI, mSchemaTable, mMetaTable, mZKMonitor).start();

      mCloser.registerPhantomRefCloseable(monitor);

      return monitor;
    }
  }

  /**
   * Listens for table layout monitors being removed from a cache and closes them, if they have not
   * already been garbage collected.
   */
  private class TableLayoutMonitorRemovalListener
      implements RemovalListener<String, TableLayoutMonitor> {
    @Override
    public void onRemoval(RemovalNotification<String, TableLayoutMonitor> notification) {
      TableLayoutMonitor monitor = notification.getValue();
      if (monitor != null) {
        // Cleanup the TableLayoutMonitor if it hasn't been collected
        LOG.debug("Cleaning up TableLayoutMonitor for table {}.", notification.getKey());
        ResourceUtils.closeOrLog(mCloser.unregisterPhantomRefCloseable(monitor));
      }
    }
  }
}
