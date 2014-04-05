package org.kiji.schema.util;

import java.io.Closeable;
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

/**
 * An {@code AutoCloser} manages {@link AutoCloseable} instance cleanup. {@link AutoCloseable}
 * instances can be registered with an {@code AutoCloser} to be cleaned up when the JVM determines
 * that the object is no longer reachable.  Alternatively, {@link AutoCloser} also implements
 * {@link Closeable}, and will close all registered {@link AutoCloseable} instances upon closing.
 *
 * Registered {@link AutoCloseable} instances can be deregistered, in which case they will no longer
 * be automatically cleaned up by the auto closer.
 */
public class AutoCloser implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceUtils.class);
  private final ExecutorService mExecutorService = Executors.newSingleThreadExecutor();

  /** Ref queue which closeable phantom references will be enqueued to. */
  private final ReferenceQueue<AutoCloseable> mReferenceQueue = new ReferenceQueue<AutoCloseable>();

  /** We must hold a strong reference to each phantom ref so they are not GC'd. */
  private final Set<CloseablePhantomRef> mReferences =
      Sets.newSetFromMap(Maps.<CloseablePhantomRef, Boolean>newConcurrentMap());

  /** Holds a weak map of AutoCloseable -> CloseablePhantomRef for unregistration purposes. */
  private final ConcurrentMap<AutoCloseable, CloseablePhantomRef> mCloseables =
      new MapMaker().weakKeys().makeMap();
  private volatile boolean mIsOpen = true;

  /**
   * Create an AutoCloser instance.
   */
  public AutoCloser() {
    mExecutorService.execute(new Closer());
  }

  /**
   * Register an {@link AutoCloseable} instance to be cleaned up by this {@code AutoCloser} when the
   * {@link AutoCloseable} is determined by the JVM to no longer be reachable.
   *
   * @param autoCloseable to be unregistered by this closer.
   */
  public void registerAutoCloseable(AutoCloseable autoCloseable) {
    Preconditions.checkState(mIsOpen);
    LOG.debug("Registering AutoCloseable {}.", autoCloseable);
    CloseablePhantomRef ref = autoCloseable.getCloseablePhantomRef(mReferenceQueue);
    CloseablePhantomRef existing = mCloseables.putIfAbsent(autoCloseable, ref);
    mReferences.add(ref);
    if (existing != null) {
      LOG.warn("AutoCloseable {} is alread registered.", autoCloseable);
    }
  }

  /**
   * Unregister an {@link AutoCloseable} instance from this closer.
   *
   * @param autoCloseable to be unregistered from this closer.
   */
  public void unregisterAutoCloseable(AutoCloseable autoCloseable) {
    Preconditions.checkState(mIsOpen);
    LOG.debug("Unregistering AutoCloseable {}.", autoCloseable);
    CloseablePhantomRef reference = mCloseables.remove(autoCloseable);
    if (reference == null) {
      LOG.warn("Could not unregister {}.", autoCloseable);
    } else {
      if (!mReferences.remove(reference)) {
        LOG.warn("Could not remove reference {} from strong set.", autoCloseable);
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * Close this {@code AutoCloser}, and close any registered {@link AutoCloseable} instances.
   */
  @Override
  public void close() {
    mIsOpen = false;
    mExecutorService.shutdownNow();
    mCloseables.clear();
    for (CloseablePhantomRef reference : mReferences) {
      reference.close();
    }
    mReferences.clear();
  }

  /**
   * Task which waits for CloseablePhantomRef instances to be enqueued to the reference queue, and
   * closes them.
   */
  private class Closer implements Runnable {
    /** {@inheritDoc} */
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
