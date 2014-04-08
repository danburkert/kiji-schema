package org.kiji.schema.util;

import java.io.Closeable;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@code AutoCloser} manages {@link AutoCloseable} instance cleanup. {@link AutoCloseable}
 * instances can be registered with an {@code AutoCloser} to be cleaned up when the JVM determines
 * that the object is no longer reachable.  Alternatively, {@link AutoCloser} also implements
 * {@link Closeable}, and will close all registered {@link AutoCloseable} instances upon closing.
 */
public class AutoCloser implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AutoCloser.class);
  private static final AtomicInteger COUNTER = new AtomicInteger(0);
  private final ExecutorService mExecutorService =
      Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
              .setNameFormat("AutoCloser-" + COUNTER.getAndIncrement())
              .build()
      );

  /** Ref queue which closeable phantom references will be enqueued to. */
  private final ReferenceQueue<AutoCloseable> mReferenceQueue = new ReferenceQueue<AutoCloseable>();

  /** We must hold a strong reference to each phantom ref so they are not GC'd. */
  private final Set<CloseablePhantomRef> mReferences =
      Sets.newSetFromMap(Maps.<CloseablePhantomRef, Boolean>newConcurrentMap());
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
    mReferences.add(
        new CloseablePhantomRef(
            autoCloseable,
            mReferenceQueue,
            autoCloseable.getCloseableResources()));
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
          LOG.info("Waiting for enqueued CloseablePhantomRefs...");
          CloseablePhantomRef reference = (CloseablePhantomRef) mReferenceQueue.remove();
          LOG.info("CloseablePhantomRef {} enqueued.", reference);
          if (mReferences.remove(reference)) {
            reference.clear(); // allows referent to be claimed by the GC.
            reference.close();
          } else {
            LOG.info("Phantom reference to unregistered AutoCloseable queued.");
          }
        }
      } catch (InterruptedException e) {
        // If this thread is interrupted, then die.
      }
    }
  }

  /**
   * A {@link PhantomReference} which implements the {@link Closeable} interface. This phantom
   * reference can be used to close resources when the referent is determined to no longer be
   * reachable.
   */
  public static class CloseablePhantomRef
      extends PhantomReference<AutoCloseable>
      implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CloseablePhantomRef.class);
    private final Collection<Closeable> mCloseables;

    /**
     * Create a {@code CloseablePhantomRef} for the provided referent and queue which will close
     * the provided {@link Closeable} resources when closed.
     *
     * @param referent of this phantom reference.
     * @param refQueue to which this reference will be enqueued when the JVM determines the referent
     *                 is no longer reachable.
     * @param closeables to be closed when {@link #close()} is called on this reference.
     */
    public CloseablePhantomRef(
        AutoCloseable referent,
        ReferenceQueue<AutoCloseable> refQueue,
        Collection<Closeable> closeables) {
      super(referent, refQueue);
      mCloseables = closeables;
    }

    /**
     * {@inheritDoc}
     *
     * Closes the resources which are held by this {@code CloseablePhantomRef}.
     */
    @Override
    public void close() {
      LOG.info("closing phantom ref");
      for (Closeable closeable : mCloseables) {
        try {
          closeable.close();
        } catch (Throwable t) {
          LOG.error("Error while closing resource {}: {}.", closeable, t.getMessage());
        }
      }
    }
  }
}
