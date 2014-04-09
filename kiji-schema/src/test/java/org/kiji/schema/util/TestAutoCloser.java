package org.kiji.schema.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MapMaker;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAutoCloser {

  private volatile AutoReferenceCountedReaper mCloser;

  @Before
  public void setUp() throws Exception {
    mCloser = new AutoReferenceCountedReaper();
  }

  @After
  public void tearDown() throws Exception {
    mCloser.close();
  }

  @Test
  public void testCloserWillCloseAutoCloseables() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    mCloser.registerAutoReferenceCounted(new LatchAutoReferenceCountable(latch));
    System.gc(); // Force the phantom ref to be enqueued

    latch.await();
  }

  @Test
  public void testCloserWillCloseRegisteredCloseablesWhenClosed() throws Exception {
    // Good test name, or best?
    final CountDownLatch latch = new CountDownLatch(1);
    final AutoReferenceCounted closeable = new LatchAutoReferenceCountable(latch);
    mCloser.registerAutoReferenceCounted(closeable);
    mCloser.close();
    latch.await();
  }

  @Test
  public void testCloserWillCloseAutoCloseableInWeakSet() throws Exception {
    Set<AutoReferenceCounted> set =
        Collections.newSetFromMap(new MapMaker().weakKeys().<AutoReferenceCounted, Boolean>makeMap());

    final CountDownLatch latch = new CountDownLatch(1);
    AutoReferenceCounted closeable = new LatchAutoReferenceCountable(latch);

    set.add(closeable);
    mCloser.registerAutoReferenceCounted(closeable);
    closeable = null;

    System.gc();
    latch.await();
  }

  @Test
  public void testCloserWillCloseAutoCloseableInWeakCache() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);

    final LoadingCache<CountDownLatch, AutoReferenceCounted> cache = CacheBuilder
        .newBuilder()
        .weakValues()
        .build(new CacheLoader<CountDownLatch, AutoReferenceCounted>() {
          @Override
          public AutoReferenceCounted load(CountDownLatch latch) throws Exception {
            AutoReferenceCounted closeable = new LatchAutoReferenceCountable(latch);
            mCloser.registerAutoReferenceCounted(closeable);
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

    final LoadingCache<CountDownLatch, AutoReferenceCounted> cache = CacheBuilder
        .newBuilder()
        .weakValues()
        .build(new CacheLoader<CountDownLatch, AutoReferenceCounted>() {
          @Override
          public AutoReferenceCounted load(CountDownLatch latch) throws Exception {
            AutoReferenceCounted closeable = new LatchAutoReferenceCountable(latch);
            mCloser.registerAutoReferenceCounted(closeable);
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
            return new Object();
          }
        });

    int hash1 = cache.get("foo").hashCode();
    System.gc();
    int hash2 = cache.get("foo").hashCode();
    System.gc();
    Assert.assertFalse(hash1 == hash2);
  }

  private static class LatchAutoReferenceCountable implements AutoReferenceCounted {
    private final Closeable mResource;

    private LatchAutoReferenceCountable(CountDownLatch latch) {
      mResource = new LatchCloseable(latch);
    }

    @Override
    public Collection<Closeable> getCloseableResources() {
      return ImmutableList.of(mResource);
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
