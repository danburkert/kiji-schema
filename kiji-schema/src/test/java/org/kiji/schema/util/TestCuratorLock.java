package org.kiji.schema.util;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.Reaper;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.KillSession;
import org.junit.Assert;
import org.junit.Test;

import org.kiji.schema.KijiIOException;
import org.kiji.schema.layout.impl.ZooKeeperClient;

public class TestCuratorLock extends ZooKeeperTest {
//
//  /** Overly basic test for ZooKeeper locks. */
//  @Test
//  public void testCuratorLock() throws Exception {
//    final File lockDir = new File("/test-zookeeper-lock");
//    final CuratorFramework zkFramework =
//      CuratorFrameworkFactory.newClient(getZKAddress(), new ExponentialBackoffRetry(1000, 3));
//    zkFramework.start();
//    try {
//      final AtomicInteger counter = new AtomicInteger(0);
//
//      final Lock lock1 = new CuratorLock(zkFramework, lockDir.getPath());
//      final Lock lock2 = new CuratorLock(zkFramework, lockDir.getPath());
//      lock1.lock();
//
//      final Thread thread = new Thread() {
//        /** {@inheritDoc} */
//        @Override
//        public void run() {
//          try {
//            Assert.assertFalse(lock2.lock(0.1));
//            counter.incrementAndGet();
//            lock2.lock();
//            counter.incrementAndGet();
//            lock2.unlock();
//            counter.incrementAndGet();
//
//            lock2.lock();
//            counter.incrementAndGet();
//            lock2.unlock();
//            lock2.close();
//            counter.incrementAndGet();
//          } catch (Exception exn) {
//            throw new KijiIOException(exn);
//          }
//        }
//      };
//      thread.start();
//
//      while (counter.get() != 1) {
//        Time.sleep(0.1);  // I hate it, but I can't think anything simpler for now.
//      }
//      lock1.unlock();
//      lock1.close();
//
//      thread.join();
//      Assert.assertEquals(5, counter.get());
//
//    } finally {
//      zkFramework.close();
//    }
//  }
//
//  /** ZooKeeperLock does not throw an exception on unlock after a losing its session. */
//  @Test
//  public void testCuratorLockKillSession() throws Exception {
//    final File lockDir = new File("/test-zookeeper-lock-kill-session");
//    final CuratorFramework zkFramework =
//        CuratorFrameworkFactory.newClient(getZKAddress(), new ExponentialBackoffRetry(1000, 3));
//    zkFramework.start();
//    try {
//      final Lock lock = new CuratorLock(zkFramework, lockDir.getPath());
//      lock.lock();
//      KillSession.kill(zkFramework.getZookeeperClient().getZooKeeper(), getZKAddress());
//      lock.unlock();
//    } catch (Exception e) {
//      System.out.println(e.getClass());
//    } finally {
//      zkFramework.close();
//    }
//  }
//
//  /** ZooKeeperLock does not throw an exception on unlock after a losing its session. */
//  @Test
//  public void testReaper() throws Exception {
//    final String path = new File("/test-zookeeper-lock-kill-session").getPath();
//    final CuratorFramework client =
//        CuratorFrameworkFactory.newClient(getZKAddress(), new ExponentialBackoffRetry(1000, 3));
//    client.start();
//    try {
//      final Lock lock = new CuratorLock(client, path);
//      final Reaper reaper = new Reaper(client);
//      lock.lock();
//      KillSession.kill(client.getZookeeperClient().getZooKeeper(), getZKAddress());
//      lock.unlock();
//    } catch (Exception e) {
//      System.out.println(e.getClass());
//    } finally {
//      client.close();
//    }
//  }
//
//  /** Overly basic test for ZooKeeper locks. */
//  @Test
//  public void testCuratorLockWithZooKeeperLock() throws Exception {
//    final File lockDir = new File("/test-curator-lock-with-zookeeper-lock");
//    final CuratorFramework zkFramework =
//        CuratorFrameworkFactory.newClient(getZKAddress(), new ExponentialBackoffRetry(1000, 3));
//    final ZooKeeperClient zk = ZooKeeperClient.getZooKeeperClient(getZKAddress());
//    zkFramework.start();
//    try {
//      final Lock lock1 = new CuratorLock(zkFramework, lockDir.getPath());
//      final Lock lock2 = new ZooKeeperLock(zk, lockDir);
//      lock2.lock();
//      lock1.lock();
//      lock2.unlock();
//      lock1.unlock();
//    } finally {
//      zkFramework.close();
//      zk.release();
//    }
//  }
}
