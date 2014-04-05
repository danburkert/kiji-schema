package org.kiji.schema.layout.impl;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.layout.KijiTableLayouts;

public class TestInstanceMonitor extends KijiClientTest {

  private TableLayoutDesc mLayout;
  private ZooKeeperClient mZKClient;
  private ZooKeeperMonitor mZKMonitor;
  private InstanceMonitor mInstanceMonitor;

  @Before
  public void setUp() throws Exception {
    mLayout = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    Kiji kiji = getKiji();
    kiji.createTable(mLayout);
    mZKClient = HBaseFactory.Provider.get().getZooKeeperClient(kiji.getURI());
    mZKMonitor = new ZooKeeperMonitor(mZKClient);

    mInstanceMonitor = new InstanceMonitor(
        "user",
        kiji.getSystemTable().getDataVersion(),
        kiji.getURI(),
        kiji.getSchemaTable(),
        kiji.getMetaTable(),
        mZKMonitor);
  }

  @After
  public void tearDown() throws Exception {
    mZKMonitor.close();
    mZKClient.release();
    mInstanceMonitor.close();
  }

  @Test
  public void testCanRetrieveTableMonitor() throws Exception {
    TableLayoutMonitor monitor = mInstanceMonitor.getTableLayoutMonitor(mLayout.getName());
    Assert.assertEquals("layout-1.0", monitor.getLayoutCapsule().getLayout().getDesc().getVersion());
  }

  @Test
  public void testClosingInstanceMonitorWillCloseTableLayoutMonitor() throws Exception {
    TableLayoutMonitor monitor = mInstanceMonitor.getTableLayoutMonitor(mLayout.getName());
    mInstanceMonitor.close();
    // TODO: figure out how to test the monitor is closed.
  }

  @Test
  public void testLosingReferenceToTableLayoutMonitorWillCloseIt() throws Exception {
    String name = mLayout.getName();
    int hash1 = mInstanceMonitor.getTableLayoutMonitor(name).hashCode();

    System.gc();
    System.gc();
    System.gc();
    System.gc();

    int hash2 = mInstanceMonitor.getTableLayoutMonitor(name).hashCode();
    System.out.println(hash1);
    System.out.println(hash2);

    Assert.assertTrue(hash1 != hash2);
  }
}
