package org.kiji.schema.impl.cassandra;

import java.io.IOException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import org.junit.Test;

import org.kiji.schema.cassandra.CassandraKijiClientTest;

public class CassandraKijiResultUtilsTest extends CassandraKijiClientTest {

  @Test
  public void testFoo() throws IOException {

    final CassandraKiji kiji = (CassandraKiji) getKiji();
    final CassandraAdmin admin = kiji.getCassandraAdmin();
    admin.execute("CREATE TABLE t (a int, b int, c int, PRIMARY KEY (a, b));");

    admin.execute("INSERT INTO t (a, b, c) values (1, 2, 3);");
    admin.execute("INSERT INTO t (a, b, c) values (1, 1, 4);");
    admin.execute("INSERT INTO t (a, b, c) values (0, 0, 0);");
    admin.execute("INSERT INTO t (a, b, c) values (1, 2, 2);");


    final ResultSet resultSet = admin.execute("SELECT * FROM t where a = 3;");

    for (Row row : resultSet) {
      System.out.println("row: " + row);

    }


  }

}
