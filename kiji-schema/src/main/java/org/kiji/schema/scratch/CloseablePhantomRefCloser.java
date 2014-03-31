package org.kiji.schema.scratch;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.util.ResourceUtils;

public class CloseablePhantomRefCloser implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);
  private final ExecutorService mExecutorService;
  private final ReferenceQueue<PhantomRefCloseable> mReferenceQueue;
  private final ConcurrentMap<Integer, CloseablePhantomRef> mReferences = Maps.newConcurrentMap();

  public CloseablePhantomRefCloser() {
    mExecutorService = Executors.newSingleThreadExecutor();
    mReferenceQueue = new ReferenceQueue<PhantomRefCloseable>();
    mExecutorService.execute(new Closer());
  }

  public void registerPhantomRefCloseable(PhantomRefCloseable phantomRefCloseable) {
    LOG.debug("Registering PhantomRefCloseable {}.", phantomRefCloseable);
    CloseablePhantomRef ref = phantomRefCloseable.getCloseablePhantomRef(mReferenceQueue);
    CloseablePhantomRef existing =
        mReferences.putIfAbsent(System.identityHashCode(phantomRefCloseable), ref);
    if (existing != null) {
      LOG.warn("Attempt to register {} failed: a PhantomRefCloseable with the same identity hash" +
          " code is already registered.", phantomRefCloseable);
    }
  }

  /**
   * Unregister a PhantomRefCloseable instance from this closer, and return its associated
   * CloseablePhantomRef instance, or null if this closer did not have the phantom ref closeable
   * registered.
   *
   * @param phantomRefCloseable to be unregistered by this closer.
   * @return the CloseablePhantomRef associated with the phantom ref closeable, or null if it was
   *         not registered.
   */
  public CloseablePhantomRef unregisterPhantomRefCloseable(PhantomRefCloseable phantomRefCloseable) {
    LOG.debug("Unregistering PhantomRefCloseable {}.", phantomRefCloseable);
    CloseablePhantomRef removed =
        mReferences.remove(System.identityHashCode(phantomRefCloseable));
    if (removed == null) {
      LOG.warn("Could not unregister {}.", phantomRefCloseable);
    }
    return removed;
  }

  @Override
  public void close() throws IOException {
    mExecutorService.shutdownNow();
  }

  public class Closer implements Runnable {
    @Override
    public void run() {
      try {
        while (true) {
          LOG.debug("Waiting for enqueued CloseablePhantomReferences...");
          CloseablePhantomRef closeable = (CloseablePhantomRef) mReferenceQueue.remove();
          LOG.debug("CloseablePhantomRef {} enqueued.", closeable);
          boolean removed = mReferences.values().remove(closeable);
          if (removed) {
            ResourceUtils.closeOrLog(closeable);
          } else {
            LOG.debug("Phantom reference to unregistered CloseablePhantomRef queued.");
          }
        }
      } catch (InterruptedException e) {
        // If this thread is interrupted, then die.
      }
    }
  }
}
