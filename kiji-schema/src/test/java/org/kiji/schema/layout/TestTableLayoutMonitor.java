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

package org.kiji.schema.layout;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiURI;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.util.ZooKeeperTest;
import org.kiji.schema.zookeeper.TableLayoutTracker;
import org.kiji.schema.zookeeper.TableUserRegistration;
import org.kiji.schema.zookeeper.TableUsersTracker;
import org.kiji.schema.zookeeper.TableUsersUpdateHandler;
import org.kiji.schema.zookeeper.TestTableLayoutTracker.QueuingTableLayoutUpdateHandler;
import org.kiji.schema.zookeeper.ZooKeeperUtils;

/** Tests for TableLayoutMonitor. */
public class TestTableLayoutMonitor extends ZooKeeperTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestTableLayoutMonitor.class);

  /** Tests the layout update notification mechanism. */
  @Test
  public void testLayoutUpdateNotification() throws Exception {
    final CuratorFramework zkClient = ZooKeeperUtils.getZooKeeperClient(getZKAddress());
    try {

      final KijiURI tableURI =
          KijiURI
              .newBuilder(String.format("kiji://%s/kiji_instance/table_name", getZKAddress()))
              .build();

      ZooKeeperUtils.setTableLayout(zkClient, tableURI, "layout.v1");

      final BlockingQueue<String> queue = Queues.newSynchronousQueue();

      final TableLayoutTracker layoutTracker =
          new TableLayoutTracker(zkClient, tableURI, new QueuingTableLayoutUpdateHandler(queue));
      try {
        layoutTracker.start();

        Assert.assertEquals("layout.v1", queue.poll(5, TimeUnit.SECONDS));

        ZooKeeperUtils.setTableLayout(zkClient, tableURI, "layout.v2");
        Assert.assertEquals("layout.v2", queue.poll(5, TimeUnit.SECONDS));

      } finally {
        layoutTracker.close();
      }
    } finally {
      zkClient.close();
    }
  }

  /** Tests the tracking of table users. */
  @Test
  public void testUsersTracker() throws Exception {
    final CuratorFramework zkClient = ZooKeeperUtils.getZooKeeperClient(getZKAddress());
    try {
      final KijiURI tableURI =
          KijiURI
              .newBuilder(String.format("kiji://%s/kiji_instance/table_name", getZKAddress()))
              .build();

      final BlockingQueue<Multimap<String, String>> queue = Queues.newSynchronousQueue();

      final TableUsersTracker tracker = new TableUsersTracker(
          zkClient,
          tableURI,
          new TableUsersUpdateHandler() {
            @Override
            public void update(Multimap<String, String> users) {
              LOG.info("Users map updated to: {}", users);
              try {
                queue.put(users);
              } catch (InterruptedException ie) {
                throw new RuntimeInterruptedException(ie);
              }
            }
          });
      tracker.start();
      final TableUserRegistration userRegistration1 =
          new TableUserRegistration(zkClient, tableURI, "user-id-1");
      final TableUserRegistration userRegistration2 =
          new TableUserRegistration(zkClient, tableURI, "user-id-2");
      try {
        Assert.assertTrue(queue.take().isEmpty());

        userRegistration1.start("layout-id-1");
        Assert.assertEquals(ImmutableSetMultimap.of("user-id-1", "layout-id-1"), queue.take());

        userRegistration1.updateLayoutID("layout-id-2");
        Assert.assertEquals(
            ImmutableSetMultimap.<String, String>of(), // unregistration event
            queue.take());
        Assert.assertEquals(
            ImmutableSetMultimap.of("user-id-1", "layout-id-2"), // re-registration event
            queue.take());

        userRegistration2.start("layout-id-1");
        Assert.assertEquals(
            ImmutableSetMultimap.of(
                "user-id-1", "layout-id-2",
                "user-id-2", "layout-id-1"),
            queue.take());

        userRegistration1.close();
        Assert.assertEquals(
            ImmutableSetMultimap.of("user-id-2", "layout-id-1"),
            queue.take());

      } finally {
        tracker.close();
        userRegistration2.close();
      }
    } finally {
      zkClient.close();
    }
  }
}
