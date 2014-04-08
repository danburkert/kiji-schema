package org.kiji.schema.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

/**
 * An {@code AutoCloseable} is an object which can be registered with an {@link AutoCloser} instance
 * to have resources closed prior to being garbage collected. {@code AutoCloseable} is similar to
 * reference counting, except that the reference counting is performed by the JVM garbage collector;
 * users of {@code AutoCloseable} objects need only to register the object with an
 * {@link AutoCloser} upon creation, no other life-cycle management needs to be done.
 *
 * {@code AutoCloseable} and {@link AutoCloser} use the {@link PhantomReference} mechanism of the
 * JVM to ensure that no threads have a reference to the {@code AutoCloseable} before closing the
 * resources of the {@code AutoCloseable}.
 *
 * To fulfill the {@code AutoCloseable} interface, classes must define the
 * {@code AutoCloseable#getCloseablePhantomRef} method, which takes a reference queue and returns
 * a {@link org.kiji.schema.util.AutoCloseable.CloseablePhantomRef}. Typically, this is as simple as
 * creating a CloseablePhantomRef with each {@link java.io.Closeable} resource:
 *
 *  public CloseablePhantomRef getCloseablePhantomRef(ReferenceQueue<AutoCloseable> queue) {
 *    return new CloseablePhantomRef(this, queue, closeableResource1, closeableResource2, ...);
 *  }
 *
 * The {@code CloseablePhantomRef} will close each resource when the JVM determines that the
 * {@code AutoCloseable} instance is no longer reachable.
 *
 * *Special Note* because cleanup relies on the {@code AutoCloseable} no longer being reachable,
 * users of {@code AutoCloseable} instances must take special care to hold a strong reference to
 * instances while in use, and to let go of the reference after the AutoCloseable is no longer
 * needed. Non-static inner-classes hold a reference to their outer class, so the same care
 * should be taken with non-static inner-class instances of an {@code AutoCloseable}.
 */
public interface AutoCloseable {

  /**
   * Returns a {@code CloseablePhantomRef} which, when closed, closes the resources held by this
   * instance.
   *
   * @param queue which the closeable phantom ref should be registered to.
   * @return a {@code CloseablePhantomRef} registered to the provided reference queue.
   */
  public CloseablePhantomRef getCloseablePhantomRef(ReferenceQueue<AutoCloseable> queue);

  /**
   * A {@link PhantomReference} which implements the {@link Closeable} interface. This phantom
   * reference can be used to close resources when the referent is determined to no longer be
   * reachable.
   */
  public static class CloseablePhantomRef
      extends PhantomReference<AutoCloseable>
      implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CloseablePhantomRef.class);
    private final Closeable[] mCloseables;

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
        Closeable... closeables) {
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
          ResourceUtils.closeOrLog(closeable);
        } catch (Throwable t) {
          LOG.warn("caught exception while closing {}: {}.", closeable, t);
        }
      }
    }
  }
}
