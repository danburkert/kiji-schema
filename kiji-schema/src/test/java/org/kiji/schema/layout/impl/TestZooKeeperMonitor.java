package org.kiji.schema.layout.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.KillSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kiji.schema.RuntimeInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableUserRegistrationDesc;
import org.kiji.schema.util.Lock;
import org.kiji.schema.util.ZooKeeperTest;

import static org.kiji.schema.layout.impl.ZooKeeperMonitor.*;

public class TestZooKeeperMonitor extends ZooKeeperTest {
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

  /***************
   * ZooKeeperLock
   ***************/

  @Test
  public void testLockIsExclusiveInThread() throws Exception {
    final String path = "/path";
    final Lock lock1 = ZooKeeperMonitor.newLock(mZKClient, path);
    final Lock lock2 = ZooKeeperMonitor.newLock(mZKClient, path);
    lock1.lock();
    Assert.assertFalse(lock2.lock(0.01)); // 10ms
    lock1.unlock();
    lock1.close();
    lock2.close();
  }

  @Test
  public void testLockIsExclusiveInterThread() throws Exception {
    final String path = "/path";
    final int concurrency = 10; // Number of threads to simulate
    final AtomicLong counter = new AtomicLong(0); // Simulates an exclusive resource
    final CountDownLatch latch = new CountDownLatch(concurrency); // Syncs threads at start
    final ListeningExecutorService executor = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(concurrency));
    final List<ListenableFuture<Void>> futures = Lists.newArrayList();

    for (int i = 0; i < concurrency; i++) {
      futures.add(
          executor.submit(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  Lock lock = ZooKeeperMonitor.newLock(mZKClient, path);
                  latch.countDown();
                  latch.await();
                  lock.lock();
                  // Check that no one else holds the lock at the same time
                  long beforeCount = counter.get();
                  Thread.sleep(1); // Critical section
                  long afterCount = counter.incrementAndGet();
                  lock.unlock();
                  Assert.assertEquals(beforeCount + 1, afterCount);
                  lock.close();
                  return null;
                }
              }
          )
      );
    }
    Futures.allAsList(futures).get(); // Will blow up if any of the futures failed
  }

  @Test(expected = IOException.class)
  public void testZooKeeperLockFailsOnSessionLoss() throws Exception {
    final String path = "/path";
    final Lock lock = ZooKeeperMonitor.newLock(mZKClient, path);
    lock.lock();
    KillSession.kill(mZKClient.getZookeeperClient().getZooKeeper(), getZKAddress());
    lock.unlock();
    lock.close();
  }

  /***************
   * LayoutTracker
   ***************/

  @Test
  public void testLayoutTrackerUpdatesCache() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";
    ZooKeeperMonitor.setTableLayout(mZKClient, tableURI, layout1);
    final CountDownLatch latch = new CountDownLatch(2);
    // It's not easy to test this without either stalling the thread for some fixed amount of time,
    // or taking advantage of the callback, which unfortunately conflates this test with the
    // callback tests.
    LayoutTracker tracker = ZooKeeperMonitor.newTableLayoutTracker(mZKClient, tableURI,
        new LayoutUpdateHandler() {
          @Override
          public void update(String layout) {
            latch.countDown();
          }
        });
    tracker.start();
    Assert.assertEquals(layout1, tracker.getLayoutID());
    ZooKeeperMonitor.setTableLayout(mZKClient, tableURI, layout2);
    latch.await(); // Wait for the update to take
    Assert.assertEquals(layout2, tracker.getLayoutID());
    tracker.close();
  }

  @Test
  public void testLayoutTrackerNotifiesHandler() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";
    ZooKeeperMonitor.setTableLayout(mZKClient, tableURI, layout1);
    final CountDownLatch latch = new CountDownLatch(2);
    final AtomicReference<String> latestLayout = new AtomicReference<String>(null);

    LayoutTracker tracker = ZooKeeperMonitor.newTableLayoutTracker(mZKClient, tableURI,
        new LayoutUpdateHandler() {
          @Override
          public void update(String layout) {
            latestLayout.set(layout);
            latch.countDown();
          }
        });
    tracker.start();
    ZooKeeperMonitor.setTableLayout(mZKClient, tableURI, layout2);
    latch.await(); // Wait for the update to take
    Assert.assertEquals(layout2, latestLayout.get());
    tracker.close();
  }

  @Test
  public void testLayoutTrackerHandlesSessionLoss() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final String layout1 = "layout-id-1";
    final String layout2 = "layout-id-2";
    ZooKeeperMonitor.setTableLayout(mZKClient, tableURI, layout1);
    final CountDownLatch latch = new CountDownLatch(2);
    final AtomicReference<String> latestLayout = new AtomicReference<String>(null);

    LayoutTracker tracker = ZooKeeperMonitor.newTableLayoutTracker(mZKClient, tableURI,
        new LayoutUpdateHandler() {
          @Override
          public void update(String layout) {
            latestLayout.set(layout);
            latch.countDown();
          }
        });
    tracker.start();
    KillSession.kill(mZKClient.getZookeeperClient().getZooKeeper(), getZKAddress()); // Kill!
    ZooKeeperMonitor.setTableLayout(mZKClient, tableURI, layout2);
    KillSession.kill(mZKClient.getZookeeperClient().getZooKeeper(), getZKAddress()); // Kill!
    latch.await(); // Wait for the update to take
    Assert.assertEquals(layout2, latestLayout.get());
    Assert.assertEquals(layout2, tracker.getLayoutID());
    tracker.close();
  }

  /*******************************************
   * TableUserRegistration & TableUsersTracker
   *******************************************/

  @Test
  public void testTableUsersTrackerNotifiesHandlerOnStart() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final TableUserRegistrationDesc registration =
        TableUserRegistrationDesc
            .newBuilder()
            .setUserId("user-id")
            .setLayoutId("layout-id")
            .build();
    TableUserRegistration user =
        ZooKeeperMonitor.newTableUserRegistration(mZKClient, tableURI).start(registration);

    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Multimap<String, String>> latestUsers =
        new AtomicReference<Multimap<String, String>>(null);

    TableUsersTracker tracker = ZooKeeperMonitor.newTableUsersTracker(mZKClient, tableURI,
        new TableRegistrationHandler() {
          @Override
          public void update(Multimap<String, String> users) {
            latestUsers.set(users);
            latch.countDown();
          }
        });
    tracker.start();

    latch.await();
    Assert.assertEquals(ImmutableSetMultimap.of("user-id", "layout-id"), latestUsers.get());
    tracker.close();
    user.close();
  }

  @Test
  public void testTableUsersTrackerNotifiesHandlerOnNewUserRegistration() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final TableUserRegistrationDesc registration1 =
        TableUserRegistrationDesc
            .newBuilder()
            .setUserId("user-id-1")
            .setLayoutId("layout-id-1")
            .build();
    TableUserRegistration user1 =
        ZooKeeperMonitor.newTableUserRegistration(mZKClient, tableURI).start(registration1);

    final CountDownLatch latch = new CountDownLatch(2);
    final AtomicReference<Multimap<String, String>> latestUsers =
        new AtomicReference<Multimap<String, String>>(null);

    TableUsersTracker tracker = ZooKeeperMonitor.newTableUsersTracker(mZKClient, tableURI,
        new TableRegistrationHandler() {
          @Override
          public void update(Multimap<String, String> users) {
            latestUsers.set(users);
            latch.countDown();
          }
        });
    tracker.start();

    final TableUserRegistrationDesc registration2 =
        TableUserRegistrationDesc
            .newBuilder()
            .setUserId("user-id-2")
            .setLayoutId("layout-id-2")
            .build();
    TableUserRegistration user2 =
        ZooKeeperMonitor.newTableUserRegistration(mZKClient, tableURI).start(registration2);

    latch.await();
    Assert.assertEquals(
        ImmutableSetMultimap.of("user-id-1", "layout-id-1", "user-id-2", "layout-id-2"),
        latestUsers.get());
    tracker.close();
    user1.close();
    user2.close();
  }

  @Test
  public void testTableUsersTrackerNotifiesHandlerOnUserDeregistration() throws Exception {
    final KijiURI tableURI = KijiURI.newBuilder().withInstanceName("i").withTableName("t").build();
    final TableUserRegistrationDesc registration =
        TableUserRegistrationDesc
            .newBuilder()
            .setUserId("user-id")
            .setLayoutId("layout-id")
            .build();
    TableUserRegistration user =
        ZooKeeperMonitor.newTableUserRegistration(mZKClient, tableURI).start(registration);

    final CountDownLatch latch = new CountDownLatch(2);
    final AtomicReference<Multimap<String, String>> latestUsers =
        new AtomicReference<Multimap<String, String>>(null);

    TableUsersTracker tracker = ZooKeeperMonitor.newTableUsersTracker(mZKClient, tableURI,
        new TableRegistrationHandler() {
          @Override
          public void update(Multimap<String, String> users) {
            latestUsers.set(users);
            latch.countDown();
          }
        });
    tracker.start();
    user.close();

    latch.await();
    Assert.assertEquals(ImmutableSetMultimap.of(), latestUsers.get());
    tracker.close();
  }

  @Test
  public void testTableUsersTrackerMultipleUsers() throws Exception {
    final KijiURI tableURI =
        KijiURI
            .newBuilder(String.format("kiji://%s/kiji_instance/table_name", getZKAddress()))
            .build();

    final BlockingQueue<Multimap<String, String>> queue = Queues.newArrayBlockingQueue(10);

    final ZooKeeperMonitor.TableUsersTracker tracker = ZooKeeperMonitor.newTableUsersTracker(
        mZKClient,
        tableURI,
        new ZooKeeperMonitor.TableRegistrationHandler() {
          @Override
          public void update(Multimap<String, String> users) {
            try {
              queue.put(users);
            } catch (InterruptedException ie) {
              throw new RuntimeInterruptedException(ie);
            }
          }
        });
    tracker.start();
    Assert.assertTrue(queue.take().isEmpty());

    TableUserRegistration user1 =
        ZooKeeperMonitor.newTableUserRegistration(mZKClient, tableURI);
    user1.start(TableUserRegistrationDesc
        .newBuilder()
        .setUserId("user-id-1")
        .setLayoutId("layout-id-1")
        .build());
    Assert.assertEquals(ImmutableSetMultimap.of("user-id-1", "layout-id-1"), queue.take());

    TableUserRegistration user2 =
        ZooKeeperMonitor.newTableUserRegistration(mZKClient, tableURI);
    user2.start(TableUserRegistrationDesc
        .newBuilder()
        .setUserId("user-id-2")
        .setLayoutId("layout-id-2")
        .build());
    Assert.assertEquals(
        ImmutableSetMultimap.of(
            "user-id-1", "layout-id-1",
            "user-id-2", "layout-id-2"),
        queue.take());

    TableUserRegistration user3 =
        ZooKeeperMonitor.newTableUserRegistration(mZKClient, tableURI);
    user3.start(TableUserRegistrationDesc
        .newBuilder()
        .setUserId("user-id-3")
        .setLayoutId("layout-id-3")
        .build());
    Assert.assertEquals(
        ImmutableSetMultimap.of(
            "user-id-1", "layout-id-1",
            "user-id-2", "layout-id-2",
            "user-id-3", "layout-id-3"),
        queue.take());

    user3.close();

    Assert.assertEquals(
        ImmutableSetMultimap.of("user-id-1", "layout-id-1", "user-id-2", "layout-id-2"),
        queue.take());

    // No more events
    Assert.assertTrue(queue.isEmpty());

    tracker.close();
    user1.close();
    user2.close();
  }

  @Test
  public void testTableUsersRegistrationHandlesSessionLoss() throws Exception {
    final KijiURI tableURI = KijiURI
        .newBuilder(String.format("kiji://%s/kiji_instance/table_name", getZKAddress()))
        .build();

    final BlockingQueue<Multimap<String, String>> queue = Queues.newArrayBlockingQueue(10);
    CuratorFramework zkClient2 =
        CuratorFrameworkFactory.newClient(getZKAddress(), new ExponentialBackoffRetry(1000, 3));
    zkClient2.start();

    final ZooKeeperMonitor.TableUsersTracker tracker = ZooKeeperMonitor.newTableUsersTracker(
        mZKClient,
        tableURI,
        new ZooKeeperMonitor.TableRegistrationHandler() {
          @Override
          public void update(Multimap<String, String> users) {
            try {
              queue.put(users);
            } catch (InterruptedException ie) {
              throw new RuntimeInterruptedException(ie);
            }
          }
        });
    tracker.start();
    // No users initially
    Assert.assertTrue(queue.take().isEmpty());

    TableUserRegistration user =
        ZooKeeperMonitor.newTableUserRegistration(zkClient2, tableURI);
    user.start(TableUserRegistrationDesc
        .newBuilder()
        .setUserId("user-id")
        .setLayoutId("layout-id")
        .build());

    // user is registered
    Assert.assertEquals(ImmutableSetMultimap.of("user-id", "layout-id"), queue.take());

    // user registration session loss
    KillSession.kill(zkClient2.getZookeeperClient().getZooKeeper(), getZKAddress());

    // user disappears during session loss
    Assert.assertEquals(ImmutableSetMultimap.of(), queue.take());

    // and reappears with new session connection
    Assert.assertEquals(ImmutableSetMultimap.of("user-id", "layout-id"), queue.take());

    // No more events
    Assert.assertTrue(queue.isEmpty());

    tracker.close();
    user.close();
    zkClient2.close();
  }


  @Test
  public void testTableUsersTrackerHandlesSessionLoss() throws Exception {
    final KijiURI tableURI = KijiURI
        .newBuilder(String.format("kiji://%s/kiji_instance/table_name", getZKAddress()))
        .build();

    final BlockingQueue<Multimap<String, String>> queue = Queues.newArrayBlockingQueue(10);
    CuratorFramework zkClient2 =
        CuratorFrameworkFactory.newClient(getZKAddress(), new ExponentialBackoffRetry(1000, 3));
    zkClient2.start();

    final ZooKeeperMonitor.TableUsersTracker tracker = ZooKeeperMonitor.newTableUsersTracker(
        zkClient2,
        tableURI,
        new ZooKeeperMonitor.TableRegistrationHandler() {
          @Override
          public void update(Multimap<String, String> users) {
            try {
              System.out.println(users);
              queue.put(users);
            } catch (InterruptedException ie) {
              throw new RuntimeInterruptedException(ie);
            }
          }
        });
    tracker.start();
    // No users initially
    Assert.assertTrue(queue.take().isEmpty());

    TableUserRegistration user =
        ZooKeeperMonitor.newTableUserRegistration(mZKClient, tableURI);
    user.start(TableUserRegistrationDesc
        .newBuilder()
        .setUserId("user-id")
        .setLayoutId("layout-id")
        .build());

    // user is registered
    Assert.assertEquals(ImmutableSetMultimap.of("user-id", "layout-id"), queue.take());

    // user tracker session loss
    KillSession.kill(zkClient2.getZookeeperClient().getZooKeeper(), getZKAddress());

    // handler triggered on new session connection
    Assert.assertEquals(ImmutableSetMultimap.of("user-id", "layout-id"), queue.take());

    // No more events
    Assert.assertTrue(queue.isEmpty());

    tracker.close();
    user.close();
    zkClient2.close();
  }
}
