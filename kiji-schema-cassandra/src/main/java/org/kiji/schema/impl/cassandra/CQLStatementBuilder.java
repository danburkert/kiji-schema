package org.kiji.schema.impl.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.layout.KijiTableLayout;

/**
 *
 */
public class CQLStatementBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(CQLStatementBuilder.class);

  // Useful static members for referring to different fields in the C* tables.
  public static final String RAW_KEY_COL = "key";         // Only used for tables with raw eids
  public static final String KEY_COMPONENT_PREFIX = "eid_";
  public static final String LOCALITY_GROUP_COL = "lg";   // Only used for counter tables
  public static final String FAMILY_COL = "family";
  public static final String QUALIFIER_COL = "qualifier";
  public static final String VERSION_COL = "version";     // Only used for locality group tables
  public static final String VALUE_COL = "value";

  public static enum ColumnType {
    BYTES("blob"),
    STRING("varchar"),
    INT("int"),
    LONG("bigint"),
    COUNTER("counter");

    private final String mName;

    /**
     * Default constructor.
     *
     * @param name The CQL name for the column type.
     */
    ColumnType(String name) {
      mName = name;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return mName;
    }
  }

  private final KijiTableLayout mLayout;


  public CQLStatementBuilder(final KijiTableLayout layout) {
    mLayout = layout;
  }
}
