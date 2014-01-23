package org.kiji.schema;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.kiji.schema.impl.AsyncHBaseKiji;
import org.kiji.schema.impl.AsyncHBaseKijiTable;
import org.kiji.schema.impl.AsyncHBaseKijiTableReader;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;

import java.util.List;

public class AsyncTest extends KijiClientTest {

  public static void main(String[] args) throws Exception {
    KijiURI uri = KijiURI.newBuilder("kiji://.env/default").build();
    Kiji backing = Kiji.Factory.open(uri);
    AsyncHBaseKiji kiji = new AsyncHBaseKiji(backing);

    String family = "family";
    String qualifier = "column";


    new InstanceBuilder(kiji)
        .withTable(KijiTableLayouts.getLayout("org/kiji/schema/layout/formattedkey.json"))

          .withRow("a", "b", "c", 1, 0L)
            .withFamily(family)
              .withQualifier(qualifier)
                .withValue("hello, world!")

          .withRow("a", "b", "c", 1, 1L)
            .withFamily(family)
              .withQualifier(qualifier)
                .withValue("hello, foo!")

        .withRow("a", "b", "c", 1, 2L)
          .withFamily(family)
            .withQualifier(qualifier)
              .withValue("hello, bar!")

        .withRow("a", "b", "c", 1, 3L)
          .withFamily(family)
            .withQualifier(qualifier)
              .withValue("hello, baz!")

        .withRow("a", "b", "c", 1, 4L)
          .withFamily(family)
            .withQualifier(qualifier)
              .withValue("shutup, baz!")

        .build();

    AsyncHBaseKijiTable table = kiji.openTable("table");

    AsyncHBaseKijiTableReader reader = table.openTableReader();

    KijiDataRequest dataRequest = KijiDataRequest
        .builder()
        .addColumns(
            KijiDataRequestBuilder.ColumnsDef.create()
              .withMaxVersions(999)
              .add(family, qualifier)
              .add(family, qualifier))
        .build();
    EntityId eid = table.getEntityId("my-row");

    Deferred<List<AsyncHBaseKijiTableReader.KijiKeyValue<?>>> deferred = reader.flatAsyncGet(eid, dataRequest);

    deferred.addCallback(new Callback<Object, List<AsyncHBaseKijiTableReader.KijiKeyValue<?>>>() {
      @Override
      public Object call(List<AsyncHBaseKijiTableReader.KijiKeyValue<?>> kvs) throws Exception {
        for(AsyncHBaseKijiTableReader.KijiKeyValue<?> kv : kvs) {
          System.out.println(kv);
        }
        return null;
      }
    }).join();
  }
}
