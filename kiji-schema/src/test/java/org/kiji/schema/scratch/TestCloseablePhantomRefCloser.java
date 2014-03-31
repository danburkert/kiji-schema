package org.kiji.schema.scratch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.util.concurrent.CountDownLatch;

public class TestCloseablePhantomRefCloser {

  private volatile CloseablePhantomRefCloser mCloser;

  @Before
  public void setUp() throws Exception {
    mCloser = new CloseablePhantomRefCloser();
  }

  @After
  public void tearDown() throws Exception {
    mCloser.close();
  }

  @Test
  public void testCloserWillClosePhantomRefCloseables() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    mCloser.registerPhantomRefCloseable(new LatchPhantomRefCloseable(latch));
    System.gc(); // Force the phantom ref to be enqueued

    latch.await();
  }

  private static class LatchPhantomRefCloseable implements PhantomRefCloseable {
    private final Closeable mResource;

    private LatchPhantomRefCloseable(CountDownLatch latch) {
      mResource = new LatchCloseable(latch);
    }

    @Override
    public CloseablePhantomRef getCloseablePhantomRef(ReferenceQueue<PhantomRefCloseable> queue) {
      return new CloseablePhantomRef(this, queue, mResource);
    }
  }

  private static class LatchCloseable implements Closeable {
    private final CountDownLatch mLatch;

    private LatchCloseable(CountDownLatch latch) {
      mLatch = latch;
    }

    @Override
    public void close() throws IOException {
      mLatch.countDown();
    }
  }
}
