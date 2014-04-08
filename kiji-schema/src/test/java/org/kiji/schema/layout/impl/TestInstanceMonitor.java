package org.kiji.schema.layout.impl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiClientTest;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.hbase.HBaseFactory;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.UsersTracker;

public class TestInstanceMonitor extends KijiClientTest {


  private final String userName = "user";
  private volatile KijiURI mTableURI;
  private volatile ZooKeeperClient mZKClient;
  private volatile ZooKeeperMonitor mZKMonitor;
  private volatile InstanceMonitor mInstanceMonitor;

  @Before
  public void setUp() throws Exception {
    Kiji kiji = getKiji();
    TableLayoutDesc layout = KijiTableLayouts.getLayout(KijiTableLayouts.SIMPLE);
    kiji.createTable(layout);
    mTableURI = KijiURI.newBuilder(kiji.getURI()).withTableName(layout.getName()).build();
    mZKClient = HBaseFactory.Provider.get().getZooKeeperClient(kiji.getURI());
    mZKMonitor = new ZooKeeperMonitor(mZKClient);

    mInstanceMonitor = new InstanceMonitor(
        userName,
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
    TableLayoutMonitor monitor = mInstanceMonitor.getTableLayoutMonitor(mTableURI.getTable());
    Assert.assertEquals("layout-1.0", monitor.getLayoutCapsule().getLayout().getDesc().getVersion());
  }

  @Test
  public void testClosingInstanceMonitorWillCloseTableLayoutMonitor() throws Exception {
    TableLayoutMonitor monitor = mInstanceMonitor.getTableLayoutMonitor(mTableURI.getTable());
    mInstanceMonitor.close();
    // TODO: figure out how to test the monitor is closed.
  }

  @Test
  public void testLosingReferenceToTableLayoutMonitorWillCloseIt() throws Exception {
    int hash1 = mInstanceMonitor.getTableLayoutMonitor(mTableURI.getTable()).hashCode();
    System.gc();
    int hash2 = mInstanceMonitor.getTableLayoutMonitor(mTableURI.getTable()).hashCode();
    Assert.assertTrue(hash1 != hash2);
  }

  @Test
  public void testLosingReferenceToTableLayoutMonitorWillUpdateZooKeeper() throws Exception {
    final BlockingQueue<Multimap<String, String>> usersQueue = Queues.newSynchronousQueue();
    UsersTracker tracker =
        mZKMonitor.newTableUsersTracker(mTableURI, new TestZooKeeperMonitor.QueueingUsersUpdateHandler(usersQueue));
    try {
      tracker.open();
      Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
          usersQueue.poll(1, TimeUnit.SECONDS));

      mInstanceMonitor.getTableLayoutMonitor(mTableURI.getTable());

      Assert.assertEquals(ImmutableSetMultimap.of(userName, "1"),
          usersQueue.poll(1, TimeUnit.SECONDS));

      System.gc();

      Assert.assertEquals(ImmutableSetMultimap.<String, String>of(),
          usersQueue.poll(1, TimeUnit.SECONDS));

    } finally {
      tracker.close();
    }

  }
}
