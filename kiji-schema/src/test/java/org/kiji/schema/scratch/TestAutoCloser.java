package org.kiji.schema.scratch;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.MapMaker;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.kiji.schema.util.AutoCloseable;
import org.kiji.schema.util.AutoCloser;


public class TestAutoCloser {

  private volatile AutoCloser mCloser;

  @Before
  public void setUp() throws Exception {
    mCloser = new AutoCloser();
  }

  @After
  public void tearDown() throws Exception {
    mCloser.close();
  }

  @Test
  public void testCloserWillCloseAutoCloseables() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    mCloser.registerAutoCloseable(new LatchAutoCloseable(latch));
    System.gc(); // Force the phantom ref to be enqueued

    latch.await();
  }

  @Test
  public void testCloserCanUnregisterAutoCloseable() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    AutoCloseable closeable = new LatchAutoCloseable(latch);
    mCloser.registerAutoCloseable(closeable);
    Assert.assertNotNull("Unable to unregister closeable.",
        mCloser.unregisterAutoCloseable(closeable));
    closeable = null;
    System.gc();
    Assert.assertEquals("AutoCloser will not close unregistered closeable.", 1, latch.getCount());
  }

  @Test
  public void testCloserWillCloseRegisteredCloseablesWhenClosed() throws Exception {
    // Good test name, or best?
    final CountDownLatch latch = new CountDownLatch(1);
    final AutoCloseable closeable = new LatchAutoCloseable(latch);
    mCloser.registerAutoCloseable(closeable);
    mCloser.close();
    latch.await();
  }

  @Test
  public void testCloserWillCloseAutoCloseableInWeakSet() throws Exception {
    Set<org.kiji.schema.util.AutoCloseable> set =
        Collections.newSetFromMap(new MapMaker().weakKeys().<AutoCloseable, Boolean>makeMap());

    final CountDownLatch latch = new CountDownLatch(1);
    AutoCloseable closeable = new LatchAutoCloseable(latch);

    set.add(closeable);
    mCloser.registerAutoCloseable(closeable);
    closeable = null;

    System.gc();
    latch.await();
  }

  @Test
  public void testCloserWillCloseAutoCloseableInWeakCache() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    final LoadingCache<CountDownLatch, AutoCloseable> cache = CacheBuilder
        .newBuilder()
        .weakValues()
        .build(new CacheLoader<CountDownLatch, AutoCloseable>() {
          @Override
          public AutoCloseable load(CountDownLatch latch) throws Exception {
            AutoCloseable closeable = new LatchAutoCloseable(latch);
            mCloser.registerAutoCloseable(closeable);
            return closeable;
          }
        });

    cache.get(latch); // force creation

    System.gc();
    latch.await();
  }

  private static class LatchAutoCloseable implements AutoCloseable {
    private final Closeable mResource;

    private LatchAutoCloseable(CountDownLatch latch) {
      mResource = new LatchCloseable(latch);
    }

    @Override
    public CloseablePhantomRef getCloseablePhantomRef(ReferenceQueue<AutoCloseable> queue) {
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
