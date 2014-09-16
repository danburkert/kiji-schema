package org.kiji.schema.impl.cassandra;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiResult;
import org.kiji.schema.KijiResultScanner;
import org.kiji.schema.layout.CassandraColumnNameTranslator;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.impl.CellDecoderProvider;

/**
 * {@inheritDoc}
 *
 * Cassandra implementation of {@code KijiResultScanner}.
 *
 * @param <T> type of {@code KijiCell} value returned by scanned {@code KijiResult}s.
 */
public class CassandraKijiResultScanner<T> implements KijiResultScanner<T> {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiResultScanner.class);


  /*
  * ## Implementation Notes
  *
  * To perform a scan over a Kiji table,
  */



  public CassandraKijiResultScanner(
      final KijiDataRequest request,
      final CassandraKijiTable table,
      final KijiTableLayout layout,
      final CellDecoderProvider decoderProvider,
      final CassandraColumnNameTranslator translator
  ) throws IOException {




    for (final Column column : request.getColumns()) {
      layout.getCellSpec(column.getColumnName()).isCounter();
    }
  }


  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public KijiResult<T> next() {
    return null;
  }

  @Override
  public void remove() {

  }
}
