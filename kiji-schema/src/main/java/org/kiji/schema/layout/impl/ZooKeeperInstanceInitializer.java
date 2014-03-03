package org.kiji.schema.layout.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;

/**
 * Initializes ZooKeeper to a consistent state upon startup, if needed.  ZooKeeper state can be
 * rebuilt from the master copy in HBase.
 */
public class ZooKeeperInstanceInitializer {

  private final CuratorFramework mZKClient;
  private final Kiji mKiji;


  public ZooKeeperInstanceInitializer(CuratorFramework zkCLient, Kiji kiji) {
    mZKClient = zkCLient;
    mKiji = kiji;
  }

  public void ensureInitialized() throws Exception {
    KijiURI instanceURI = mKiji.getURI();

    /* /kiji-schema/instances/{instance}/users */
    ensureNode(ZooKeeperMonitor.getInstanceUsersDir(instanceURI).getPath());

    for (String tableName : mKiji.getTableNames()) {
      KijiURI tableURI = KijiURI.newBuilder(instanceURI).withTableName(tableName).build();
      String layoutId = mKiji.getMetaTable().getTableLayout(tableName).getDesc().getLayoutId();

      /* /kiji-schema/instances/{instance}/tables/{table}/layout */
      ensureNode(ZooKeeperMonitor.getTableLayoutFile(tableURI).getPath(), Bytes.toBytes(layoutId));
    }
  }

  /**
   * Ensures that the specified node exists.
   *
   * @param path of the node.
   * @throws Exception on unrecoverable ZooKeeper error.
   */
  private void ensureNode(String path) throws Exception {
    try {
      mZKClient.create().creatingParentsIfNeeded().forPath(path);
    } catch (KeeperException.NodeExistsException e) {
      // already exists
    }
  }

  /**
   * Ensures that the specified node exists.  If it does not, it is created with the specified data.
   * Note that if the node exists, the data is not compared or updated.
   *
   * @param path of the node.
   * @param data to create the node with.
   * @throws Exception on unrecoverable ZooKeeper error.
   */
  private void ensureNode(String path, byte[] data) throws Exception {
    try {
      mZKClient.create().creatingParentsIfNeeded().forPath(path, data);
    } catch (KeeperException.NodeExistsException e) {
      // already exists
    }
  }
}
