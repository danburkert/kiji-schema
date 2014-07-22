package org.kiji.schema.impl.cassandra;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiURI;
import org.kiji.schema.avro.SchemaType;
import org.kiji.schema.cassandra.CassandraTableName;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout;
import org.kiji.schema.layout.KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout;
import org.kiji.schema.layout.impl.ColumnId;

/**
 * Temporary class to hold miscellaneous Cassandra KijiResult code.
 */
public class CassandraKijiResultUtils {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraKijiResultUtils.class);


  /**
   * Translates a {@link KijiDataRequest} into a map of {@link CassandraTableName} to column
   * requests.
   *
   * @param tableURI The table URI of the Kiji table.
   * @param dataRequest The data request describing the get or scan on the table.
   * @param layout The Kiji table layout of the table.
   * @return A map of Cassandra table name to column requests.
   */
  public static ListMultimap<CassandraTableName, Column> decomposeDataRequest(
      final KijiURI tableURI,
      final KijiDataRequest dataRequest,
      final KijiTableLayout layout
  ) {
    final ImmutableListMultimap.Builder<CassandraTableName, Column> columnRequests =
        ImmutableListMultimap.builder();

    for (final Column columnRequest : dataRequest.getColumns()) {
      final KijiColumnName column = columnRequest.getColumnName();
      final FamilyLayout family = layout.getFamilyMap().get(column.getFamily());
      if (column.isFullyQualified()) {
        final ColumnLayout columnLayout = family.getColumnMap().get(column.getQualifier());
        if (columnLayout.getDesc().getColumnSchema().getType() == SchemaType.COUNTER) {
          columnRequests.put(CassandraTableName.getCounterTableName(tableURI), columnRequest);
        } else {
          final ColumnId localityGroupID = family.getLocalityGroup().getId();
          columnRequests.put(
              CassandraTableName.getLocalityGroupTableName(tableURI, localityGroupID),
              columnRequest);
        }
      } else if (family.isMapType()) {
        if (family.getDesc().getMapSchema().getType() == SchemaType.COUNTER) {
          columnRequests.put(CassandraTableName.getCounterTableName(tableURI), columnRequest);
        } else {
          final ColumnId localityGroupID = family.getLocalityGroup().getId();
          columnRequests.put(
              CassandraTableName.getLocalityGroupTableName(tableURI, localityGroupID),
              columnRequest);
        }
      } else { // group type family
        for (final ColumnLayout columnLayout : family.getColumns()) {
          if (columnLayout.getDesc().getColumnSchema().getType() == SchemaType.COUNTER) {
            columnRequests.put(CassandraTableName.getCounterTableName(tableURI), columnRequest);
          } else {
            final ColumnId localityGroupID = family.getLocalityGroup().getId();
            columnRequests.put(
                CassandraTableName.getLocalityGroupTableName(tableURI, localityGroupID),
                columnRequest);
          }
        }
      }
    }

    return columnRequests.build();
  }
}
