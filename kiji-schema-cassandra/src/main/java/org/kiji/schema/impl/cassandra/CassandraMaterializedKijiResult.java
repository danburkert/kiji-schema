package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiResult;

/**
 *
 */
public class CassandraMaterializedKijiResult<T> implements KijiResult<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraMaterializedKijiResult.class);

  @Override
  public EntityId getEntityId() {
    return null;
  }

  @Override
  public KijiDataRequest getDataRequest() {
    return null;
  }

  @Override
  public Iterator<KijiCell<T>> iterator() {
    return null;
  }

  @Override
  public <U extends T> KijiResult<U> narrowView(final KijiColumnName column) {
    return null;
  }

  @Override
  public void close() throws IOException {

  }
}
