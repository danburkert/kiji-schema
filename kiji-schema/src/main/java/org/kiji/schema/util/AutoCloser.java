package org.kiji.schema.util;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Preconditions;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.util.AutoCloseable.CloseablePhantomRef;

public class AutoCloser implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);
  private final ExecutorService mExecutorService = Executors.newSingleThreadExecutor();
  private final ReferenceQueue<AutoCloseable> mReferenceQueue = new ReferenceQueue<AutoCloseable>();
  private final Set<CloseablePhantomRef> mReferences =
      Sets.newSetFromMap(Maps.<CloseablePhantomRef, Boolean>newConcurrentMap());
  private final ConcurrentMap<AutoCloseable, CloseablePhantomRef> mCloseables =
      new MapMaker().weakKeys().makeMap();
  private volatile boolean isOpen = true;

  public AutoCloser() {
    mExecutorService.execute(new Closer());
  }

  public CloseablePhantomRef registerAutoCloseable(AutoCloseable autoCloseable) {
    Preconditions.checkState(isOpen);
    LOG.debug("Registering AutoCloseable {}.", autoCloseable);
    CloseablePhantomRef ref = autoCloseable.getCloseablePhantomRef(mReferenceQueue);
    CloseablePhantomRef existing = mCloseables.putIfAbsent(autoCloseable, ref);
    mReferences.add(ref);
    if (existing != null) {
      LOG.warn("Attempt to register {} failed: a AutoCloseable with the same identity hash" +
          " code is already registered.", autoCloseable);
    }
    return ref;
  }

  /**
   * Unregister an AutoCloseable instance from this closer, and return its associated
   * CloseablePhantomRef instance, or null if this closer did not have the auto closeable
   * registered.
   *
   * @param autoCloseable to be unregistered by this closer.
   * @return the CloseablePhantomRef of the auto closeable, or null if it was not registered.
   */
  public CloseablePhantomRef unregisterAutoCloseable(AutoCloseable autoCloseable) {
    Preconditions.checkState(isOpen);
    LOG.debug("Unregistering AutoCloseable {}.", autoCloseable);
    CloseablePhantomRef reference = mCloseables.remove(autoCloseable);
    if (reference == null) {
      LOG.warn("Could not unregister {}.", autoCloseable);
    } else {
      if (!mReferences.remove(reference)) {
        LOG.warn("Could not remove reference {} from strong set.", autoCloseable);
      }
    }
    return reference;
  }

  @Override
  public void close() throws IOException {
    isOpen = false;
    mExecutorService.shutdownNow();
    mCloseables.clear();
    for (CloseablePhantomRef reference : mReferences) {
      reference.close();
    }
    mReferences.clear();
  }

  private class Closer implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          LOG.debug("Waiting for enqueued CloseablePhantomRefs...");
          CloseablePhantomRef reference = (CloseablePhantomRef) mReferenceQueue.remove();
          LOG.debug("CloseablePhantomRef {} enqueued.", reference);
          boolean removed = mReferences.remove(reference);
          if (removed) {
            ResourceUtils.closeOrLog(reference);
          } else {
            LOG.debug("Phantom reference to unregistered AutoCloseable queued.");
          }
        }
      } catch (InterruptedException e) {
        // If this thread is interrupted, then die.
      }
    }
  }
}
