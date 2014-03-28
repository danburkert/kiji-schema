package org.kiji.schema.layout.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.kiji.schema.Kiji;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.ZooKeeperTest;

public class TestInstanceMonitor extends ZooKeeperTest {

  private TableLayoutDesc mLayout;
  private ZooKeeperClient mZKClient;
  private InstanceMonitor mInstanceMonitor;

  @Before
  public void setUp() throws Exception {
    mLayout = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    Kiji kiji = getKiji();
    kiji.createTable(mLayout);
    mZKClient = ZooKeeperClient.getZooKeeperClient(getZKAddress());

    mInstanceMonitor = new InstanceMonitor(
        "user",
        kiji.getSystemTable().getDataVersion(),
        kiji.getURI(),
        kiji.getSchemaTable(),
        kiji.getMetaTable(),
        new ZooKeeperMonitor(mZKClient));
  }

  @After
  public void tearDown() throws Exception {
    mZKClient.release();
    mInstanceMonitor.close();
  }

  @Test
  public void testCanRetrieveTableMonitor() throws Exception {
    TableLayoutMonitor monitor = mInstanceMonitor.getTableLayoutMonitor(mLayout.getName());
  }
}
