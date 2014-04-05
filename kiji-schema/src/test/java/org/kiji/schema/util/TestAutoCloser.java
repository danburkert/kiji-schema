package org.kiji.schema.util;

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
    mCloser.unregisterAutoCloseable(closeable);
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

  @Test
  public void testWeakCacheWillBeCleanedUp() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);

    final LoadingCache<CountDownLatch, AutoCloseable> cache = CacheBuilder
        .newBuilder()
        .weakValues()
        .build(new CacheLoader<CountDownLatch, AutoCloseable>() {
          @Override
          public AutoCloseable load(CountDownLatch latch) throws Exception {
            System.out.println("creating new AutoCloseable.");
            AutoCloseable closeable = new LatchAutoCloseable(latch);
            mCloser.registerAutoCloseable(closeable);
            return closeable;
          }
        });

    cache.get(latch);
    System.gc();
    cache.get(latch);
    System.gc();
    latch.await();
  }

  @Test
  public void testWeakCacheWillBeCleanedUp2() throws Exception {
    final LoadingCache<String, Object> cache = CacheBuilder
        .newBuilder()
        .weakValues()
        .build(new CacheLoader<String, Object>() {
          @Override
          public Object load(String key) throws Exception {
            System.out.println("creating new Object.");
            return new Object();
          }
        });

    int hash1 = cache.get("foo").hashCode();
    System.gc();
    int hash2 = cache.get("foo").hashCode();
    System.gc();
    System.out.println(hash1);
    System.out.println(hash2);
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
