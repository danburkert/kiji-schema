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

package org.kiji.schema.impl;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Queues;
import org.junit.Test;
import org.kiji.schema.KijiTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.Kiji;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.TableLayoutDesc;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.layout.impl.TestZooKeeperMonitor;
import org.kiji.schema.layout.impl.ZooKeeperClient;
import org.kiji.schema.layout.impl.ZooKeeperMonitor.UsersTracker;
import org.kiji.schema.layout.impl.ZooKeeperMonitor;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

public class IntegrationTestTableLayoutUpdate extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestTableLayoutUpdate.class);

  @Test
  public void testUpdateLayout() throws Exception {

    final KijiURI uri = getKijiURI();
    final String tableName = "IntegrationTestTableLayoutUpdate";
    final KijiURI tableURI = KijiURI.newBuilder(uri).withTableName(tableName).build();
    final String layoutId1 = "1";
    final String layoutId2 = "2";
    final HBaseKiji kiji = (HBaseKiji) Kiji.Factory.open(uri);

    try {
      final TableLayoutDesc layoutDesc = KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
      layoutDesc.setLayoutId(layoutId1);
      layoutDesc.setName(tableName);
      kiji.createTable(layoutDesc);

      final ZooKeeperClient zkClient = kiji.getZKClient().retain();
      final ZooKeeperMonitor zkMonitor = new ZooKeeperMonitor(zkClient);
      try {
        final BlockingQueue<Multimap<String, String>> queue = Queues.newArrayBlockingQueue(10);

        final UsersTracker tracker = zkMonitor.newTableUsersTracker(tableURI,
            new TestZooKeeperMonitor.QueueingUsersUpdateHandler(queue));

        tracker.open();
        try {
          // Initial user map should be empty:
          assertEquals(ImmutableSetMultimap.<String, String>of(), queue.poll(2, TimeUnit.SECONDS));

          KijiTable kijiTable = kiji.openTable(tableName);
          try {
            // We opened a table, user map must contain exactly one entry:
            assertEquals(ImmutableSet.of(layoutId1), ImmutableSet.copyOf(queue.poll(2, TimeUnit.SECONDS)
                                                                              .values()));

              // Push a layout update (a no-op, but with a new layout ID):
              final TableLayoutDesc newLayoutDesc =
                  KijiTableLayouts.getLayout(KijiTableLayouts.FOO_TEST);
              newLayoutDesc.setReferenceLayout(layoutId1);
              newLayoutDesc.setLayoutId(layoutId2);
              newLayoutDesc.setName(tableName);

              kiji.modifyTableLayout(newLayoutDesc);

              // The new user map should eventually reflect the new layout ID.
              // We opened one table, user map must contain exactly one entry:
              assertEquals(ImmutableSet.of(layoutId2),
                  ImmutableSet.copyOf(queue.poll(2, TimeUnit.SECONDS).values()));

          } finally {
            kijiTable.release();
            kijiTable = null;
          }

          System.gc();

          // Table is now closed, the user map should become empty:
          assertEquals(ImmutableSetMultimap.<String, String>of(), queue.poll(2, TimeUnit.SECONDS));
        } finally {
          tracker.close();
        }
      } finally {
        zkMonitor.close();
        zkClient.release();
        }
      } finally {
      kiji.release();
    }
  }
}


