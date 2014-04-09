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
 * An {@code AutoReferenceCountedReaper} manages {@link AutoReferenceCounted} instance cleanup.
 * {@link AutoReferenceCounted} instances can be registered with an
 * {@code AutoReferenceCountedReaper} to be cleaned up when the JVM determines that the object is
 * no longer reachable. {@link AutoReferenceCountedReaper} implements {@link Closeable}, and
 * should be closed when no longer needed. Any outstanding registered {@link AutoReferenceCounted}
 * will be reaped when the {@link AutoReferenceCountedReaper} is closed.
 */
public class AutoReferenceCountedReaper implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AutoReferenceCountedReaper.class);
  private static final AtomicInteger COUNTER = new AtomicInteger(0);
  private final ExecutorService mExecutorService =
      Executors.newSingleThreadExecutor(
          new ThreadFactoryBuilder()
              .setNameFormat("AutoReferenceCountedReaper-" + COUNTER.getAndIncrement())
              .build()
      );

  /** Ref queue which closeable phantom references will be enqueued to. */
  private final ReferenceQueue<AutoReferenceCounted> mReferenceQueue = new ReferenceQueue<AutoReferenceCounted>();

  /** We must hold a strong reference to each phantom ref so they are not GC'd. */
  private final Set<CloseablePhantomRef> mReferences =
      Sets.newSetFromMap(Maps.<CloseablePhantomRef, Boolean>newConcurrentMap());
  private volatile boolean mIsOpen = true;

  /**
   * Create an AutoReferenceCountedReaper instance.
   */
  public AutoReferenceCountedReaper() {
    mExecutorService.execute(new Closer());
  }

  /**
   * Register an {@link AutoReferenceCounted} instance to be cleaned up by this
   * {@code AutoReferenceCountedReaper} when the {@link AutoReferenceCounted} is determined by
   * the JVM to no longer be reachable.
   *
   * @param autoReferenceCountable to be registered to this reaper.
   */
  public void registerAutoReferenceCounted(AutoReferenceCounted autoReferenceCountable) {
    Preconditions.checkState(mIsOpen);
    LOG.debug("Registering AutoReferenceCounted {}.", autoReferenceCountable);
    mReferences.add(
        new CloseablePhantomRef(
            autoReferenceCountable,
            mReferenceQueue,
            autoReferenceCountable.getCloseableResources()));
  }

  /**
   * {@inheritDoc}
   *
   * Close this {@code AutoReferenceCountedReaper}, and close any registered {@link AutoReferenceCounted} instances.
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
            LOG.info("Phantom reference to unregistered AutoReferenceCounted queued.");
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
      extends PhantomReference<AutoReferenceCounted>
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
        AutoReferenceCounted referent,
        ReferenceQueue<AutoReferenceCounted> refQueue,
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
