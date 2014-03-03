package org.kiji.schema.layout.impl;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.util.ZooKeeperTest;

public class TestTableLayoutMonitor extends ZooKeeperTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestZooKeeperMonitor.class);
  private volatile CuratorFramework mZKClient;

  @Before
  public void setup() throws Exception {
    mZKClient =
        CuratorFrameworkFactory.newClient(getZKAddress(), new ExponentialBackoffRetry(1000, 3));
    mZKClient.start();
  }

  @After
  public void tearDown() throws Exception {
    mZKClient.close();
  }



}
