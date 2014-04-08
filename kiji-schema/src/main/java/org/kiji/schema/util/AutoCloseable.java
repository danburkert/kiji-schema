package org.kiji.schema.util;

import java.io.Closeable;
import java.lang.ref.PhantomReference;
import java.util.Collection;

/**
 * An {@code AutoCloseable} is an object which can be registered with an {@link AutoCloser} instance
 * to have resources closed prior to being garbage collected. {@code AutoCloseable} is similar to
 * reference counting, except that the reference counting is performed by the JVM garbage collector;
 * users of {@code AutoCloseable} objects need only to register the object with an
 * {@link AutoCloser} upon creation, no other life-cycle management needs to be done.  The objects
 * resources will automatically be released when the {@Code AutoCloseable} is claimed by the
 * garbage collector.
 *
 * {@code AutoCloseable} and {@link AutoCloser} use the {@link PhantomReference} mechanism of the
 * JVM to ensure that no threads have a reference to the {@code AutoCloseable} before closing the
 * resources of the {@code AutoCloseable}.
 *
 * To fulfill the {@code AutoCloseable} interface, classes must define the
 * {@code AutoCloseable#getCloseableResources} method, which must return the Closeable resources
 * held by the instance.
 *
 * The {@code CloseablePhantomRef} will close each Closeable when the JVM determines that the
 * {@code AutoCloseable} instance is no longer reachable.
 *
 * *Special Note* because cleanup relies on the {@code AutoCloseable} no longer being reachable,
 * users of {@code AutoCloseable} instances must take special care to hold a strong reference to
 * instances while in use, and to let go of the reference after the AutoCloseable is no longer
 * needed. Non-static inner-classes hold a reference to their outer class, so the same care
 * should be taken with non-static inner-class instances of an {@code AutoCloseable}. In particular,
 * do *not* return a non-static inner class from {@code #getCloseableResources}, as this will
 * cause the {@code AutoCloseable} to never be collected due to a cyclical reference.
 */
public interface AutoCloseable {

  /**
   * Returns a collection of Closeable resources held by this AutoCloseable which must be closed
   * in order to properly dispose of this object.
   *
   * @return the Closeable resources held by this {@code AutoCloseable}.
   */
  public Collection<Closeable> getCloseableResources();
}
