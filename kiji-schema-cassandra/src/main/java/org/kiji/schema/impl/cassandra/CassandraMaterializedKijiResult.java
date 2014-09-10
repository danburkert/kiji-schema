package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiURI;
import org.kiji.schema.impl.MaterializedKijiResult;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/**
 *
 */
public class CassandraMaterializedKijiResult {

  /**
   * Create a {@code CassandraMaterializedKijiResult} for a get on a Cassandra Kiji table.
   *
   * @param entityId
   * @param dataRequest
   * @param layout
   * @param translator
   * @param decoderProvider
   * @param admin
   * @param <T>
   * @return
   */
  public static <T> MaterializedKijiResult<T> create(
      final KijiURI tableURI,
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider,
      final CassandraAdmin admin
  ) throws IOException {

    SortedMap<KijiColumnName, ListenableFuture<Iterator<KijiCell<T>>>> results =
        Maps.newTreeMap();

    for (final Column columnRequest : dataRequest.getColumns()) {
      Preconditions.checkArgument(
          !columnRequest.isPagingEnabled(),
          "CassandraMaterializedKijiResult can not be created with a paged data request: %s.",
          dataRequest);

      results.put(
          columnRequest.getColumnName(),
          CassandraKijiResult.<T>getColumn(
              tableURI,
              entityId,
              columnRequest,
              dataRequest,
              layout,
              translator,
              decoderProvider,
              admin));
    }

    final SortedMap<KijiColumnName, List<KijiCell<T>>> columns = Maps.transformValues(
        results,
        new Function<ListenableFuture<Iterator<KijiCell<T>>>, List<KijiCell<T>>>() {
          @Override
          public List<KijiCell<T>> apply(final ListenableFuture<Iterator<KijiCell<T>>> future) {
            return ImmutableList.copyOf(CassandraKijiResult.unwrapFuture(future));
          }
        });

    return MaterializedKijiResult.create(
        entityId,
        dataRequest,
        columns);
  }
}
