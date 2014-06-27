package org.kiji.schema.impl;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
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
 * Here be dragons.
 */
public class KijiResultQualifierPager implements KijiPager {
  private static final Logger LOG = LoggerFactory.getLogger(KijiResultQualifierPager.class);

  private final EntityId mEntityId;
  private final KijiDataRequest mRequest;
  private final KijiColumnName mColumn;
  private final Iterator<KijiCell<Object>> mCells;

  public KijiResultQualifierPager(
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
    final List<KijiCell<Object>> cells = Lists.newArrayList();

    KijiColumnName lastColumn = null;
    while (mCells.hasNext() && cells.size() < pageSize) {
      final KijiCell<Object> cell = mCells.next();
      if (!cell.getColumn().equals(lastColumn)) {
        cells.add(KijiCell.create(cell.getColumn(), 0, null));
      }
    }

    final KijiResult result = new MaterializedKijiResult(mEntityId, mRequest, mColumn, cells);
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
