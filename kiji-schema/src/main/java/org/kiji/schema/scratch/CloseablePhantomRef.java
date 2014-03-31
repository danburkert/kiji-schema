package org.kiji.schema.scratch;

import org.kiji.schema.util.ResourceUtils;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

public class CloseablePhantomRef extends PhantomReference<PhantomRefCloseable> implements Closeable {
  private final Closeable[] mCloseables;

  public CloseablePhantomRef(
      PhantomRefCloseable referent,
      ReferenceQueue<PhantomRefCloseable> q,
      Closeable... closeables) {
    super(referent, q);
    mCloseables = closeables;
  }

  @Override
  public void close() throws IOException {
    for (Closeable closeable : mCloseables) {
      ResourceUtils.closeOrLog(closeable);
    }
  }
}
