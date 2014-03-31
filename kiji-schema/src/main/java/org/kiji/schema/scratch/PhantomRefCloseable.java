package org.kiji.schema.scratch;

import java.lang.ref.ReferenceQueue;

public interface PhantomRefCloseable {
  public CloseablePhantomRef getCloseablePhantomRef(ReferenceQueue<PhantomRefCloseable> queue);
}
