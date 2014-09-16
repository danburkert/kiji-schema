package org.kiji.schema.impl.cassandra;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nullable;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiCell;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiURI;
import org.kiji.schema.NoSuchColumnException;
import org.kiji.schema.cassandra.CassandraColumnName;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.impl.DefaultKijiResult;
import org.kiji.schema.impl.EmptyKijiResult;
import org.kiji.schema.impl.MaterializedKijiResult;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * A utility class which can create a {@link KijiResult} view on a Cassandra Kiji table.
 */
@ApiAudience.Private
public class CassandraKijiResult {

  /**
   * Constructor for non-instantiable helper class.
   */
  private CassandraKijiResult() { }

  /**
   * Create a new {@link KijiResult} backed by Cassandra.
   *
   * @param entityId EntityId of the row from which to read cells.
   * @param dataRequest KijiDataRequest defining the values to retrieve.
   * @param table The table being viewed.
   * @param layout The layout of the table.
   * @param columnTranslator A column name translator for the table.
   * @param decoderProvider A cell decoder provider for the table.
   * @param <T> The type of value in the {@code KijiCell} of this view.
   * @return an {@code HBaseKijiResult}.
   * @throws IOException if error while decoding cells.
   */
  public static <T> KijiResult<T> create(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final CassandraKijiTable table,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator columnTranslator,
      final CellDecoderProvider decoderProvider
  ) throws IOException {
    final KijiDataRequestBuilder unpagedRequestBuilder = KijiDataRequest.builder();
    final KijiDataRequestBuilder pagedRequestBuilder = KijiDataRequest.builder();
    unpagedRequestBuilder.withTimeRange(
        dataRequest.getMinTimestamp(),
        dataRequest.getMaxTimestamp());
    pagedRequestBuilder.withTimeRange(dataRequest.getMinTimestamp(), dataRequest.getMaxTimestamp());

    for (final Column columnRequest : dataRequest.getColumns()) {
      if (columnRequest.isPagingEnabled()) {
        pagedRequestBuilder.newColumnsDef(columnRequest);
      } else {
        unpagedRequestBuilder.newColumnsDef(columnRequest);
      }
    }

    final CellDecoderProvider requestDecoderProvider =
        decoderProvider.getDecoderProviderForRequest(dataRequest);

    final KijiDataRequest unpagedRequest = unpagedRequestBuilder.build();
    final KijiDataRequest pagedRequest = pagedRequestBuilder.build();

    if (unpagedRequest.isEmpty() && pagedRequest.isEmpty()) {
      return new EmptyKijiResult<T>(entityId, dataRequest);
    }

    final MaterializedKijiResult<T> materializedKijiResult;
    if (!unpagedRequest.isEmpty()) {
      materializedKijiResult =
          CassandraMaterializedKijiResult.create(
              table.getURI(),
              entityId,
              unpagedRequest,
              layout,
              columnTranslator,
              requestDecoderProvider,
              table.getAdmin());
    } else {
      materializedKijiResult = null;
    }

    final CassandraPagedKijiResult<T> pagedKijiResult;
    if (!pagedRequest.isEmpty()) {
      pagedKijiResult =
          new CassandraPagedKijiResult<T>(
              entityId,
              pagedRequest,
              table,
              layout,
              columnTranslator,
              requestDecoderProvider);
    } else {
      pagedKijiResult = null;
    }

    if (unpagedRequest.isEmpty()) {
      return pagedKijiResult;
    } else if (pagedRequest.isEmpty()) {
      return materializedKijiResult;
    } else {
      return DefaultKijiResult.create(dataRequest, materializedKijiResult, pagedKijiResult);
    }
  }

  /**
   * Query Cassandra for a Kiji qualified-column or column-family in a Kiji row. The result is a
   * future containing an iterator over the result cells.
   *
   * @param entityId The entity ID of the row in the Kiji table.
   * @param dataRequest The data request defining the request options.
   * @param layout The table's layout.
   * @param translator A column name translator for the table.
   * @param decoderProvider A decoder provider for the table.
   * @param admin The Cassandra connection to use for querying.
   * @return A future containing an iterator of cells in the column.
   */
  public static <T> ListenableFuture<Iterator<KijiCell<T>>> getColumn(
      final KijiURI tableURI,
      final EntityId entityId,
      final Column columnRequest,
      final KijiDataRequest dataRequest,
      final KijiTableLayout layout,
      final CassandraColumnNameTranslator translator,
      final CellDecoderProvider decoderProvider,
      final CassandraAdmin admin
  ) {
    final KijiColumnName column = columnRequest.getColumnName();
    final CassandraColumnName cassandraColumn;
    try {
      cassandraColumn = translator.toCassandraColumnName(column);
    } catch (NoSuchColumnException e) {
      throw new IllegalArgumentException(
          String.format("No such column '%s' in table %s.", column, tableURI));
    }

    final ColumnId localityGroupId =
        layout.getFamilyMap().get(column.getFamily()).getLocalityGroup().getId();
    final CassandraTableName table =
        CassandraTableName.getLocalityGroupTableName(tableURI, localityGroupId);

    final Statement statement =
        CQLUtils.getColumnGetStatement(
            layout,
            table,
            entityId,
            cassandraColumn,
            dataRequest,
            columnRequest);

    final Function<Row, KijiCell<T>> rowDecoder =
        RowDecoders.getRowDecoderFunction(column, layout, translator, decoderProvider);

    return Futures.transform(
        admin.executeAsync(statement),
        new Function<ResultSet, Iterator<KijiCell<T>>>() {
          @Override
          public Iterator<KijiCell<T>> apply(final ResultSet resultSet) {
            return Iterators.transform(resultSet.iterator(), rowDecoder);
          }
        });
  }

  /**
   * Unwrap a Cassandra listenable future.
   *
   * @param future The future to unwrap.
   * @param <T> The value type of the future.
   * @return The future's value.
   */
  public static <T> T unwrapFuture(final ListenableFuture<T> future) {
    // See DefaultResultSetFuture#getUninterruptibly
    try {
      return Uninterruptibles.getUninterruptibly(future);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof DriverException) {
        throw ((DriverException) e.getCause()).copy();
      } else {
        throw new DriverInternalError("Unexpected exception thrown", e.getCause());
      }
    }
  }
}
