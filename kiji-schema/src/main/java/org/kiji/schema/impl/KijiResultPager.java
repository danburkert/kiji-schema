package org.kiji.schema.impl;

import java.io.IOException;
import java.util.Iterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiPager;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiRowData;

/**
 *
 */
public class KijiResultPager implements KijiPager {
  private static final Logger LOG = LoggerFactory.getLogger(KijiResultPager.class);

  private final EntityId mEntityId;
  private final KijiDataRequest mRequest;
  private final KijiColumnName mColumn;
  private final Iterator<KijiCell<Object>> mCells;

  public KijiResultPager(
      final EntityId entityId,
      final KijiDataRequest request,
      final KijiColumnName column,
      final Iterator<KijiCell<Object>> cells
  ) {
    mEntityId = entityId;
    mRequest = request;
    mColumn = column;
    mCells = cells;
  }

  @Override
  public KijiRowData next() {
    return next(mRequest.getColumn(mColumn).getPageSize());
  }

  @Override
  public KijiRowData next(final int pageSize) {
    final Iterator<KijiCell<Object>> iterator = Iterators.limit(mCells, pageSize);
    final KijiResult result =
        new MaterializedKijiResult(mEntityId, mRequest, mColumn, ImmutableList.copyOf(iterator));
    return new KijiResultRowData(result);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("KijiPager does not support remove.");
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
  public boolean hasNext() {
    return mCells.hasNext();
  }
}
