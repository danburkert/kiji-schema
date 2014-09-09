package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiURI;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
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

    ListMultimap<KijiColumnName, ListenableFuture<List<KijiCell<T>>>> futures =
        ArrayListMultimap.create(dataRequest.getColumns().size(), 2);

    for (final Column columnRequest : dataRequest.getColumns()) {
      Preconditions.checkArgument(
          !columnRequest.isPagingEnabled(),
          "CassandraMaterializedKijiResult can not be created with a paged data request: %s.",
          dataRequest);

      final KijiColumnName column = columnRequest.getColumnName();
      final CassandraColumnName cassandraColumn = translator.toCassandraColumnName(column);

      for (final CassandraTableName cassandraTable
          : CassandraDataRequestAdapter.getColumnCassandraTables(tableURI, layout, column)) {

        final Statement statement =
            CQLUtils.getColumnGetStatement(
                layout,
                cassandraTable,
                entityId,
                cassandraColumn,
                dataRequest,
                columnRequest);

        final Function<ResultSet, List<KijiCell<T>>> resultSetDecoder =
            RowDecoders.getResultSetDecoderFunction(
                cassandraTable,
                column,
                layout,
                translator,
                decoderProvider);

        final ListenableFuture<List<KijiCell<T>>> cells =
            Futures.transform(admin.executeAsync(statement), resultSetDecoder);

        futures.put(column, cells);
      }
    }

    final Map<KijiColumnName, List<KijiCell<T>>> columns = Maps.transformValues(
        Multimaps.asMap(futures),
        new Function<List<ListenableFuture<List<KijiCell<T>>>>, List<KijiCell<T>>>() {
          @Override
          public List<KijiCell<T>> apply(final List<ListenableFuture<List<KijiCell<T>>>> futures) {
            return FluentIterable
                .from(futures)
                .transformAndConcat(
                    new Function<ListenableFuture<List<KijiCell<T>>>, Iterable<KijiCell<T>>>() {
                      @Override
                      public Iterable<KijiCell<T>> apply(
                          final ListenableFuture<List<KijiCell<T>>> futures
                      ) {
                        // See DefaultResultSetFuture#getUninterruptibly
                        try {
                          return Uninterruptibles.getUninterruptibly(futures);
                        } catch (ExecutionException e) {
                          if (e.getCause() instanceof DriverException) {
                            throw ((DriverException)e.getCause()).copy();
                          } else {
                            throw new DriverInternalError("Unexpected exception thrown",
                                e.getCause());
                          }
                        }
                      }
                    })
                .toList();
          }
        });

    return MaterializedKijiResult.create(
        entityId,
        dataRequest,
        ImmutableSortedMap.copyOf(columns));
  }
}
