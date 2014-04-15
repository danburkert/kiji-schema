/**
 * (c) Copyright 2014 WibiData, Inc.
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

package org.kiji.schema.zookeeper;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.util.Debug;
import org.kiji.schema.util.Lock;
import org.kiji.schema.util.Time;

/** Distributed lock on top of ZooKeeper. */
@ApiAudience.Private
public final class ZooKeeperLock implements Lock, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperLock.class);
  private static final Logger CLEANUP_LOG =
      LoggerFactory.getLogger("cleanup." + ZooKeeperLock.class.getName());
  private static final String LOCK_NAME_PREFIX = "lock-";

  private final String mConstructorStack;
  private final CuratorFramework mZKClient;
  private final File mLockDir;
  private final File mLockPathPrefix;
  private File mCreatedPath = null;
  private WatchedEvent mPrecedingEvent = null;

  /**
   * Constructs a ZooKeeper lock object.
   *
   * @param zkClient ZooKeeper client.
   * @param lockDir Path of the directory node to use for the lock.
   */
  public ZooKeeperLock(CuratorFramework zkClient, File lockDir) {
    mConstructorStack = CLEANUP_LOG.isDebugEnabled() ? Debug.getStackTrace() : null;
    mZKClient = zkClient;
    mLockDir = lockDir;
    mLockPathPrefix = new File(lockDir, LOCK_NAME_PREFIX);
  }

  /** Watches the lock directory node. */
  private class LockWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      LOG.debug("ZooKeeperLock.LockWatcher: received event {}", event);
      synchronized (ZooKeeperLock.this) {
        mPrecedingEvent = event;
        ZooKeeperLock.this.notifyAll();
      }
    }
  }

  private final LockWatcher mLockWatcher = new LockWatcher();

  /** {@inheritDoc} */
  @Override
  public void lock() throws IOException {
    if (!lock(0.0)) {
      // This should never happen, instead lock(0.0) should raise a KeeperException!
      throw new RuntimeException("Unable to acquire ZooKeeper lock.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean lock(double timeout) throws IOException {
    return lockInternal(timeout);
  }

  /**
   * Acquires the lock.
   *
   * @param timeout Deadline, in seconds, to acquire the lock. 0 means no timeout.
   * @return whether the lock is acquired (ie. false means timeout).
   * @throws IOException on unrecoverable ZooKeeper error.
   */
  private boolean lockInternal(double timeout) throws IOException {
    /** Absolute time deadline, in seconds since Epoch */
    final double absoluteDeadline = (timeout > 0.0) ? Time.now() + timeout : 0.0;

    synchronized (this) {
      Preconditions.checkState(null == mCreatedPath, mCreatedPath);
      // Queues for access to the lock:
      try {
        mCreatedPath = new File(
            mZKClient
                .create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(mLockPathPrefix.getPath()));
      } catch (Exception e) {
        ZooKeeperUtils.wrapAndRethrow(e);
      }
    }

    LOG.debug("{}: queuing for lock with node {}", this, mCreatedPath);

    while (true) {
      try {
        // Do we own the lock already, or do we have to wait?
        final List<String> children = mZKClient.getChildren().forPath(mLockDir.getPath());
        Collections.sort(children);
        LOG.debug("{}: lock queue: {}", this, children);
        final int index = Collections.binarySearch(children, mCreatedPath.getName());
        if (index == 0) {
          // We own the lock:
          LOG.debug("{}: lock acquired", this);
          return true;
        } else { // index >= 1
          synchronized (this) {
            final String preceding = new File(mLockDir, children.get(index - 1)).getPath();
            LOG.debug("{}: waiting for preceding node {} to disappear", this, preceding);
            if (mZKClient.checkExists().usingWatcher(mLockWatcher).forPath(preceding) != null) {
              if (absoluteDeadline > 0.0) {
                final double timeLeft = absoluteDeadline - Time.now();
                if (timeLeft <= 0) {
                  LOG.debug("{}: out of time while acquiring lock, deleting {}",
                      this, mCreatedPath);
                  mZKClient.delete().forPath(mCreatedPath.getPath());
                  mCreatedPath = null;
                  return false;
                }
                this.wait((long) (timeLeft * 1000.0));
              } else {
                this.wait();
              }
              if ((mPrecedingEvent != null)
                  && (mPrecedingEvent.getType() == EventType.NodeDeleted)
                  && (index == 1)) {
                LOG.debug("{}: lock acquired after {} disappeared", this, preceding);
              }
            }
          }
        }
      } catch (InterruptedException ie) {
        LOG.debug("Thread interrupted while locking.");
        Thread.currentThread().interrupt();
        return false;
      } catch (Exception e) {
        ZooKeeperUtils.wrapAndRethrow(e);
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  public void unlock() throws IOException {
    unlockInternal();
  }

  /**
   * Releases the lock.
   *
   * @throws IOException on unrecoverable ZooKeeper error.
   */
  private void unlockInternal() throws IOException {
    File pathToDelete;
    synchronized (this) {
      Preconditions.checkState(null != mCreatedPath,
          "unlock() cannot be called while lock is unlocked.");
      pathToDelete = mCreatedPath;
      LOG.debug("Releasing lock {}: deleting {}", this, mCreatedPath);
      mCreatedPath = null;
    }
    try {
      mZKClient.delete().forPath(pathToDelete.getPath());
    } catch (Exception e) {
      ZooKeeperUtils.wrapAndRethrow(e);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    synchronized (this) {
      if (mCreatedPath != null) {
        CLEANUP_LOG.warn("Releasing ZooKeeperLock with path: {} in close().  "
            + "Please ensure that locks are unlocked before closing.", mCreatedPath);
        if (CLEANUP_LOG.isDebugEnabled()) {
          CLEANUP_LOG.debug("ZooKeeperLock with path '{}' was constructed through:\n{}",
              mCreatedPath, mConstructorStack);
        }
        unlock();
      }
    }
  }
}
