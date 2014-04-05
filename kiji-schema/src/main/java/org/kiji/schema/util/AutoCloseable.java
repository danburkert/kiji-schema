package org.kiji.schema.util;

import java.io.Closeable;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

public interface AutoCloseable {

  public CloseablePhantomRef getCloseablePhantomRef(ReferenceQueue<AutoCloseable> queue);

  public static class CloseablePhantomRef extends PhantomReference<AutoCloseable> implements Closeable {
    private final Closeable[] mCloseables;

    public CloseablePhantomRef(
        AutoCloseable referent,
        ReferenceQueue<AutoCloseable> q,
        Closeable... closeables) {
      super(referent, q);
      mCloseables = closeables;
    }

    @Override
    public void close() {
      for (Closeable closeable : mCloseables) {
        ResourceUtils.closeOrLog(closeable);
      }
    }
  }
}
