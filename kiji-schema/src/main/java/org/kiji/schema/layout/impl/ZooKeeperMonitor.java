/**
 * (c) Copyright 2013 WibiData, Inc.
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
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.annotations.ApiStability;
import org.kiji.annotations.Inheritance;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.InstanceUserRegistrationDesc;
import org.kiji.schema.avro.TableUserRegistrationDesc;
import org.kiji.schema.util.AvroUtils;
import org.kiji.schema.util.Lock;

/**
 * Monitor tracking table layouts.
 *
 * <p>
 *   The monitor roles include:
 *   <ul>
 *     <li> Reporting new layout updates to active users of a table:
 *       When a table's layout is being updated, users of that table will receive a notification and
 *       will automatically reload the table layout.
 *     </li>
 *     <li> Reporting active users of a table to table management client processes:
 *       Every user of a table will advertise itself as such and report the version of the table
 *       layout it currently uses.
 *       This makes it possible for a process to ensure that table users have a consistent view
 *       on the table layout before applying further updates.
 *     </li>
 *   </ul>
 * </p>
 *
 * <h2> ZooKeeper node tree structure </h2>
 *
 * <p>
 *  The monitor manages a tree of ZooKeeper nodes organized as follows:
 *  <ul>
 *    <li> {@code /kiji-schema} : Root ZooKeeper node for all Kiji instances. </li>
 *    <li> {@code /kiji-schema/instances/[instance-name]} :
 *        Root ZooKeeper node for the Kiji instance with name "instance-name".
 *    </li>
 *    <li> {@code /kiji-schema/instances/[instance-name]/tables/[table-name]} :
 *        Root ZooKeeper node for the Kiji table with name "table-name" belonging to the Kiji
 *        instance named "instance-name".
 *    </li>
 *  </ul>
 * </p>
 *
 * <h2> ZooKeeper nodes for a Kiji table </h2>
 *
 * Every table is associated with three ZooKeeper nodes:
 * <ul>
 *   <li>
 *     {@code /kiji-schema/instances/[instance-name]/tables/[table-name]/layout} :
 *     this node contains the most recent version of the table layout.
 *     Clients should watch this node for changes to be notified of table layout updates.
 *   </li>
 *   <li>
 *     {@code /kiji-schema/instances/[instance-name]/tables/[table-name]/users} :
 *     this directory contains a node for each user of the table;
 *     each user's node contains the version of the layout as seen by the client.
 *     Management tools should watch these users' nodes to ensure that all clients have a
 *     consistent view on a table's layout before/after pushing new updates.
 *   </li>
 *   <li>
 *     {@code /kiji-schema/instances/[instance-name]/tables/[table-name]/layout_update_lock} :
 *     this directory node is used to acquire exclusive lock for table layout updates.
 *     Layout management tools are required to acquire this lock before proceeding with any
 *     table layout update.
 *   </li>
 * </ul>
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public final class ZooKeeperMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperMonitor.class);

  /** Root path of the ZooKeeper directory node where to write Kiji nodes. */
  public static final File ROOT_ZOOKEEPER_PATH = new File("/kiji-schema");

  /** Path of the ZooKeeper directory where instance Kiji nodes are written. */
  public static final File INSTANCES_ZOOKEEPER_PATH = new File(ROOT_ZOOKEEPER_PATH, "instances");

  // -----------------------------------------------------------------------------------------------

  /**
   * Reports the ZooKeeper node path for a Kiji instance.
   *
   * @param kijiURI URI of a Kiji instance to report the ZooKeeper node path for.
   * @return the ZooKeeper node path for a Kiji instance.
   */
  public static File getInstanceDir(KijiURI kijiURI) {
    return new File(INSTANCES_ZOOKEEPER_PATH, kijiURI.getInstance());
  }

  /**
   * Reports the path of the ZooKeeper node for permissions changes locking.
   *
   * @param instanceURI URI of the instance for which to get a lock for permissions changes.
   * @return the path of the ZooKeeper node used as a lock for permissions changes.
   */
  public static File getInstancePermissionsLock(KijiURI instanceURI) {
    return new File(getInstanceDir(instanceURI), "permissions_lock");
  }

  /**
   * Reports the ZooKeeper root path containing all tables in a Kiji instance.
   *
   * @param kijiURI URI of a Kiji instance to report the ZooKeeper node path for.
   * @return the ZooKeeper node path that contains all the tables in the specified Kiji instance.
   */
  public static File getInstanceTablesDir(KijiURI kijiURI) {
    return new File(getInstanceDir(kijiURI), "tables");
  }

  /**
   * Reports the ZooKeeper root path containing all users of a Kiji instance.
   *
   * @param kijiURI URI of a Kiji instance to report the ZooKeeper node path for.
   * @return the ZooKeeper node path that contains all users of the specified Kiji instance.
   */
  public static File getInstanceUsersDir(KijiURI kijiURI) {
    return new File(getInstanceDir(kijiURI), "users");
  }

  /**
   * Reports the ZooKeeper node path for a Kiji table.
   *
   * @param tableURI URI of a Kiji table to report the ZooKeeper node path for.
   * @return the ZooKeeper node path for a Kiji table.
   */
  public static File getTableDir(KijiURI tableURI) {
    return new File(getInstanceTablesDir(tableURI), tableURI.getTable());
  }

  /**
   * Reports the path of the ZooKeeper node containing the most recent version of a table's layout.
   *
   * @param tableURI Reports the path of the ZooKeeper node that contains the most recent layout
   *     version of the Kiji table identified by this URI.
   * @return the path of the ZooKeeper node that contains the most recent layout version of the
   *     specified Kiji table.
   */
  public static File getTableLayoutFile(KijiURI tableURI) {
    return new File(getTableDir(tableURI), "layout");
  }

  /**
   * Reports the path of the ZooKeeper node where users of a table register themselves.
   *
   * @param tableURI Reports the path of the ZooKeeper node where users of the Kiji table with this
   *     URI register themselves.
   * @return the path of the ZooKeeper node where users of a table register.
   */
  public static File getTableUsersDir(KijiURI tableURI) {
    return new File(getTableDir(tableURI), "users");
  }

  /**
   * Reports the path of the ZooKeeper node for table layout update locking.
   *
   * @param tableURI Reports the path of the ZooKeeper node used to create locks for table layout
   *     updates.
   * @return the path of the ZooKeeper node used to create locks for table layout updates.
   */
  public static File getTableLayoutUpdateLock(KijiURI tableURI) {
    return new File(getTableDir(tableURI), "layout_update_lock");
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Private constructor.
   */
  private ZooKeeperMonitor() {
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Creates a tracker for a table layout.
   *
   * <p> The handler should not perform any blocking operations, as subsequent ZooKeeper
   * notifications will be blocked. </p>
   *
   * <p> The tracker must be opened and closed. </p>
   *
   * @param zkClient ZooKeeper client.
   * @param tableURI Tracks the layout of the table with this URI.
   * @param handler Handler invoked to process table layout updates.
   * @return a new layout tracker for the specified table.
   */
  public static LayoutTracker newTableLayoutTracker(
      CuratorFramework zkClient,
      KijiURI tableURI,
      LayoutUpdateHandler handler) {
    return new LayoutTracker(zkClient, tableURI, handler);
  }

  /**
   * Creates a lock for layout updates on the specified table.
   *
   * @param zkClient ZooKeeper client.
   * @param tableURI URI of the table to create a lock for.
   * @return a Lock for the table with the specified URI.
   *     The lock is not acquired at this point: the user must calli {@code Lock.lock()} and then
   *     release the lock with {@code Lock.unlock()}.
   */
  public static Lock newTableLayoutUpdateLock(CuratorFramework zkClient, KijiURI tableURI) {
    return new ZooKeeperLock(zkClient, getTableLayoutUpdateLock(tableURI).getPath());
  }

  /**
   * Creates a tracker for the users of a table.
   *
   * <p> The tracker must be opened and closed. </p>
   *
   * @param zkClient ZooKeeper client.
   * @param tableURI Tracks the users of the table with this URI.
   * @param handler Handler invoked to process updates to the users list of the specified table.
   * @return a new tracker for the users of the specified table.
   */
  public static TableUsersTracker newTableUsersTracker(
      CuratorFramework zkClient,
      KijiURI tableURI,
      TableRegistrationHandler handler) {
    return new TableUsersTracker(zkClient, tableURI, handler);
  }

  /**
   * Create an instance registration for the instance with the provided KijiURI. The actual
   * registration will take place once #start() is called.
   *
   * @param zkClient ZooKeeper client.
   * @param kijiURI of the instance.
   * @return an InstanceUserRegistration for the user.
   */
  public static InstanceUserRegistration newInstanceUserRegistration(
      CuratorFramework zkClient,
      KijiURI kijiURI) {
    return new InstanceUserRegistration(zkClient, kijiURI);
  }

  public static TableUserRegistration newTableUserRegistration(
      CuratorFramework zkClient,
      KijiURI tableURI) {
    return new TableUserRegistration(zkClient, tableURI);
  }

  public static Lock newLock(CuratorFramework zkClient, String path) {
    return new ZooKeeperLock(zkClient, path);
  }

  /**
   * Sets a table layout file in ZooKeeper for the table.  The caller *must* create a table layout
   * lock with {@link #getTableLayoutUpdateLock(org.kiji.schema.KijiURI)} and acquire the lock
   * before calling this, otherwise race conditions abound.
   *
   * @param tableURI of the table.
   * @param layoutID of the table's layout.
   * @throws IOException on unrecoverable ZooKeeper error.
   */
  public static void setTableLayout(
      CuratorFramework zkClient,
      KijiURI tableURI,
      String layoutID) throws IOException {
    String path = getTableLayoutFile(tableURI).getPath();
    byte[] data = Bytes.toBytes(layoutID);
    try {
      try {
        zkClient.setData().forPath(path, data);
        LOG.debug("Updating layout ID for table {} to {}.", tableURI, layoutID);
      } catch (KeeperException.NoNodeException nne) {
        zkClient.create().creatingParentsIfNeeded().forPath(path, data);
        LOG.debug("Creating layout ID node for table {} with value {}.", tableURI, layoutID);
      }
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new KijiIOException(e);
    }
  }

  /**
   * Delete the ZooKeeper record of a table.
   *
   * @param zkClient ZooKeeper client.
   * @param tableURI table to delete.
   * @throws IOException if unrecoverable ZooKeeper error.
   */
  public static void deleteTable(
      CuratorFramework zkClient,
      KijiURI tableURI
  ) throws IOException {
    deleteRecursively(zkClient, getTableDir(tableURI).getPath());
  }

  /**
   * Delete the ZooKeeper record of an instance.
   *
   * @param zkClient ZooKeeper client.
   * @param instanceURI instance to delete.
   * @throws IOException if unrecoverable ZooKeeper error.
   */
  public static void deleteInstance(
      CuratorFramework zkClient,
      KijiURI instanceURI
  ) throws IOException {
    deleteRecursively(zkClient, getInstanceDir(instanceURI).getPath());
  }

  /**
   * Delete ZooKeeper node, and any child nodes as necessary.
   */
  private static void deleteRecursively(CuratorFramework zkClient, String path) throws IOException {
    try {
      zkClient
          .delete()
          .deletingChildrenIfNeeded()
          .forPath(path);
    } catch (KeeperException.NodeExistsException e) {
      // Do nothing; the table directory doesn't exist.
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new KijiIOException(e);
    }
  }

  static void ls(CuratorFramework zkClient, String path) throws Exception {
    final List<String> children = zkClient.getChildren().forPath(path);
    LOG.info("Children of {}: {}.", path, children);
  }

  // -----------------------------------------------------------------------------------------------

  /** Interface for trackers of a table's layout. */
  public interface LayoutUpdateHandler {
    /**
     * Processes an update to the table layout.  This method should *not* block; it is executed on
     * the only ZooKeeper event processing thread.
     *
     * @param layout Layout update, as an encoded byte[].
     *     This is the update content of the layout ZooKeeper node.
     */
    void update(String layout);
  }

  /**
   * Tracks the layout of a table and reports updates to its registered handler.
   */
  public static final class LayoutTracker implements Closeable {
    private final NodeCache mCache;
    private final LayoutUpdateHandler mHandler;

    /**
     * Retrieve the current Layout ID from ZooKeeper.
     *
     * @return the current Layout ID.
     */
    public String getLayoutID() throws IOException {
      ChildData currentData = mCache.getCurrentData();
      if (currentData == null) {
        return null;
      } else {
        return Bytes.toString(currentData.getData());
      }
    }

    private LayoutTracker(
        final CuratorFramework zkClient,
        final KijiURI tableURI,
        final LayoutUpdateHandler handler) {
      mCache = new NodeCache(zkClient, getTableLayoutFile(tableURI).getPath());
      mHandler = handler;
      mCache.getListenable().addListener(new NodeCacheListener() {
        @Override
        public void nodeChanged() throws Exception {
          mHandler.update(getLayoutID());
        }
      });
    }

    /**
     * Start this layout tracker.  Updates handler with initial layout before returning.
     *
     * @throws IOException if unrecoverable ZooKeeper error while starting the tracker.
     */
    public LayoutTracker start() throws IOException {
      try {
        mCache.start(true);
        mHandler.update(getLayoutID());
        return this;
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) {
        throw new KijiIOException(e);
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      mCache.close();
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Interface for trackers of a table's users.
   */
  public interface TableRegistrationHandler {

    /**
     * Processes an update to the list of users of the Kiji table being tracked.
     *
     * @param users Updated mapping from user ID to layout IDs of the Kiji table being tracked.
     */
    void update(Multimap<String, String> users);
  }

  /**
   * Tracks registered table users.
   *
   * <p> Monitors the registered users of a table and reports updates to the registered handlers.
   * </p>
   *
   * <p> The handler should not perform any blocking operations, as subsequent ZooKeeper
   * notifications will be blocked. </p>
   */
  public static final class TableUsersTracker implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TableUsersTracker.class);
    private static final DatumReader<TableUserRegistrationDesc> DATUM_READER =
        new SpecificDatumReader<TableUserRegistrationDesc>(TableUserRegistrationDesc.class);
    private final PathChildrenCache mCache;
    private final KijiURI mTableURI;
    private final TableRegistrationHandler mHandler;

    /**
     * Initializes a new users tracker with the given update handler on the specified table. The
     * handler will be called asynchronously when the tracker is started.
     *
     * @param tableURI Tracks the users of the table with this URI.
     * @param handler Handler to process updates of the table users list.
     */
    private TableUsersTracker(
        final CuratorFramework zkClient,
        final KijiURI tableURI,
        final TableRegistrationHandler handler) {
      mTableURI = tableURI;
      mCache = new PathChildrenCache(zkClient, getTableUsersDir(mTableURI).getPath(), true);
      mHandler = handler;
      mCache.getListenable().addListener(new PathChildrenCacheListener() {
        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
            throws IOException {
          LOG.debug("Table users tracker event recieved for table {}: {}.", mTableURI, event);
          mHandler.update(getTableUsers());
        }
      });
    }

    /**
     * Retrieve a multimap of the current table users to their respective table layout version.
     *
     * @return a multimap of the current table users to their respective table layout version.
     * @throws IOException on failure.
     */
    public Multimap<String, String> getTableUsers() throws IOException {
      ImmutableMultimap.Builder<String, String> users = ImmutableSetMultimap.builder();
      for (ChildData child : mCache.getCurrentData()) {
        TableUserRegistrationDesc registration = AvroUtils.fromBytes(child.getData(), DATUM_READER);
        users.put(registration.getUserId(), registration.getLayoutId());
      }
      return users.build();
    }

    /**
     * Starts the tracker.  Will trigger an update event.
     */
    public TableUsersTracker start() throws IOException {
      LOG.debug("Starting table user tracker for table {}.", mTableURI);
      try {
        mCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        Multimap<String, String> tableUsers = getTableUsers();
        mHandler.update(tableUsers);
        return this;
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) {
        throw new KijiIOException(e);
      }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
      LOG.debug("Closing table user tracker for table {}.", mTableURI);
      mCache.close();
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * An instance user registration persisted in ZooKeeper.
   */
  public static final class InstanceUserRegistration implements Closeable {

    private static final DatumWriter<InstanceUserRegistrationDesc> DATUM_WRITER =
        new SpecificDatumWriter<InstanceUserRegistrationDesc>(InstanceUserRegistrationDesc.class);

    private final PersistentEphemeralNode mNode;

    /**
     * Create an instance registration for the instance with the provided KijiURI. The actual
     * registration will take place once #start() is called.
     *
     * @param instanceURI of instance being registered.
     */
    private InstanceUserRegistration(CuratorFramework zkClient, KijiURI instanceURI) {
      mNode = new PersistentEphemeralNode(
          zkClient,
          PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL_SEQUENTIAL,
          ZooKeeperMonitor.getInstanceUsersDir(instanceURI).getPath() + "/user",
          new byte[] { });
    }

    /**
     * Start the instance user registration with the provided registration information.
     *
     * @param instanceUserRegistrationDesc initial registration information.
     * @throws IOException if unable to register the instance user.
     */
    public void start(InstanceUserRegistrationDesc instanceUserRegistrationDesc) throws IOException {
      try {
        mNode.setData(AvroUtils.toBytes(instanceUserRegistrationDesc, DATUM_WRITER));
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) {
        throw new KijiIOException(e);
      }
      mNode.start();
    }

    /**
     * Update this instance user registration with new information.
     *
     * @param instanceUserRegistrationDesc updated registration information.
     * @throws IOException if unable to update the instance user information.
     */
    public void update(InstanceUserRegistrationDesc instanceUserRegistrationDesc)
        throws IOException {
      try {
        mNode.setData(AvroUtils.toBytes(instanceUserRegistrationDesc, DATUM_WRITER));
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) {
        throw new KijiIOException(e);
      }
    }

    /**
     * Unregister the instance user.
     *
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
      mNode.close();
    }
  }

  // -----------------------------------------------------------------------------------------------

  public static final class TableUserRegistration implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(TableUserRegistration.class);
    private static final DatumWriter<TableUserRegistrationDesc> DATUM_WRITER =
        new SpecificDatumWriter<TableUserRegistrationDesc>(TableUserRegistrationDesc.class);

    private final PersistentEphemeralNode mNode;
    private final KijiURI mTableURI;

    /**
     * Create a table registration for the table with the provided KijiURI. The actual
     * registration will take place once #start() is called.
     *
     * @param tableURI of instance being registered.
     */
    private TableUserRegistration(CuratorFramework zkClient, KijiURI tableURI) {
      mTableURI = tableURI;
      String path = ZooKeeperMonitor.getTableUsersDir(mTableURI).getPath();
      LOG.debug("Creating table user registration for table {} in path {}.", mTableURI, path);
      mNode = new PersistentEphemeralNode(
          zkClient,
          PersistentEphemeralNode.Mode.PROTECTED_EPHEMERAL_SEQUENTIAL,
          path + "/user",
          new byte[]{});
    }

    /**
     * Start the instance user registration with the provided registration information. This will
     * block the thread until the table user is successfully registered.
     *
     * @param tableUserRegistration initial registration information.
     * @throws IOException if unable to register the instance user.
     */
    public TableUserRegistration start(TableUserRegistrationDesc tableUserRegistration)
        throws IOException {
      try {
        mNode.setData(AvroUtils.toBytes(tableUserRegistration, DATUM_WRITER));
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) {
        throw new KijiIOException(e);
      }
      LOG.debug("Starting table user registration on table {} for user {} with layout id {}.",
          mTableURI, tableUserRegistration.getUserId(), tableUserRegistration.getLayoutId());
      mNode.start();
      try {
        if (!mNode.waitForInitialCreate(60, TimeUnit.SECONDS)) { // Rather arbitrary
          LOG.warn("Unable to start TableUserRegistration in 60 seconds.");
        }
      } catch (InterruptedException e) {
        LOG.warn("Interupted while waiting for TableUserRegistration start.");
      }
      return this;
    }

    /**
     * Update this instance user registration with new information.
     *
     * @param tableUserRegistration updated registration information.
     * @throws IOException if unable to update the instance user information.
     */
    public void update(TableUserRegistrationDesc tableUserRegistration) throws IOException {
      LOG.debug("Updating table user registration on table {} for user {} with layout id {}.",
          mTableURI, tableUserRegistration.getUserId(), tableUserRegistration.getLayoutId());
      try {
        mNode.setData(AvroUtils.toBytes(tableUserRegistration, DATUM_WRITER));
      } catch (IOException ioe) {
        throw ioe;
      } catch (Exception e) {
        throw new KijiIOException(e);
      }
    }

    /**
     * Unregister the instance user.
     * <p/>
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException {
      // It would be nice to log user info here, but not worth the trip to ZK
      LOG.debug("Closing table user registration on table {}.", mTableURI);
      mNode.close();
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * A synchronous, non-reentrant, distributed lock in ZooKeeper at the specified path.
   * This instance does not manage the lifecycle of the provided CuratorFramework; the lock will
   * cease to function when the CuratorFramework instance is closed.
   *
   * This lock is *not* resilient to ZooKeeper connection failures. If the session is lost
   * (expired) or suspended (unable to connect), then another client could take out the lock. As a
   * result, be careful when holding the lock for longer than the session timeout of the provided
   * client. Warnings will be logged if the ZooKeeper connection expires while the lock is taken
   * out, and if the lock is released after it has been lost due to a session expiration.
   */
  public static class ZooKeeperLock implements Lock {
    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLock.class);

    private final String mPath;

    private final InterProcessSemaphoreMutex mLock;

    private AtomicBoolean isDirty = new AtomicBoolean(false);

    private ZooKeeperLock(CuratorFramework client, String path) {
      mPath = path;
      mLock = new InterProcessSemaphoreMutex(client, path);
      client.getConnectionStateListenable().addListener(new DisconnectListener());
    }

    @Override
    public void lock() throws IOException {
      try {
        LOG.debug("Acquiring lock for path {}.", mPath);
        mLock.acquire();
        isDirty.set(false);
        LOG.debug("Lock acquired for path {}.", mPath);
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new KijiIOException(e);
      }
    }


    @Override
    public boolean lock(double timeout) throws IOException {
      try {
        LOG.debug("Acquiring lock for path {} with timeout {}.", mPath, timeout);
        boolean acquired =  mLock.acquire((long) (1000 * timeout), TimeUnit.MILLISECONDS);
        if (acquired) {
          isDirty.set(false);
          LOG.debug("Lock acquired for path {}.", mPath);
        } else {
          LOG.debug("Unable to acquire lock for path {}.", mPath);
        }
        return acquired;
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new KijiIOException(e);
      }
    }

    @Override
    public void unlock() throws IOException {
      try {
        LOG.debug("Releasing lock for path {}.", mPath);
        mLock.release();
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new KijiIOException(e);
      }
      if (isDirty.compareAndSet(true, false)) {
        // Lock is dirty
        LOG.debug("Lock is dirty for path {}.", mPath);
        throw new IOException("Lost ZooKeeper connection while locked. Lock is dirty.");
      }
    }

    @Override
    public void close() throws IOException {
      if (mLock.isAcquiredInThisProcess()) {
        LOG.warn("Attempting to close an aquired lock on {}. Please explicitly unlock the lock. " +
            "Unlocking...", mPath);
        unlock();
      }
    }

    private boolean isLocked() {
      return mLock.isAcquiredInThisProcess();
    }

    private class DisconnectListener implements ConnectionStateListener {
      @Override
      public void stateChanged(CuratorFramework client, ConnectionState newState) {
        if (isLocked()) { // We only need to worry if the lock is currently taken out
          switch (newState) {
            case SUSPENDED: {
              LOG.warn(
                  "ZooKeeper connection suspended while lock taken out with path {}.  "
                    + "The lock could possibly be lost, but this client believes it is acquired.",
                  mPath);
              // Don't set isDirty, because we may still hold the lock.
              break;
            }
            case LOST: {
              LOG.warn(
                  "ZooKeeper connection lost while lock taken out with path {}.  "
                    + "The lock is lost, but this client believes it is acquired.",
                  mPath);
              isDirty.set(true);
              break;
            }
          }
        }
      }
    }
  }
}
