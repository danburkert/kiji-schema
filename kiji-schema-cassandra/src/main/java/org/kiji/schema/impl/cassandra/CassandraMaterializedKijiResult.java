package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.SortedMap;

import com.datastax.driver.core.Row;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiResult;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.HBaseColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/**
 *
 */
public class CassandraMaterializedKijiResult<T> implements KijiResult<T> {

  private final EntityId mEntityId;
  private final KijiDataRequest mDataRequest;
  private final KijiTableLayout mLayout;
  private final CassandraColumnNameTranslator mColumnTranslator;
  private final CellDecoderProvider mDecoderProvider;
  private final SortedMap<KijiColumnName, Row> mColumnResults;

  private CassandraMaterializedKijiResult(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider,
      final SortedMap<KijiColumnName, Row> columnResults
  ) {
    mEntityId = entityId;
    mDataRequest = dataRequest;
    mLayout = layout;
    mColumnTranslator = columnTranslator;
    mDecoderProvider = decoderProvider;
    mColumnResults = columnResults;
  }

  public static <T> CassandraMaterializedKijiResult<T> create(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final SortedMap<KijiColumnName, Row> rows,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) {
    return new CassandraMaterializedKijiResult<T>(
        entityId,
        dataRequest,
        layout,
        columnTranslator,
        decoderProvider,
        columnResults.build());
  }


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
