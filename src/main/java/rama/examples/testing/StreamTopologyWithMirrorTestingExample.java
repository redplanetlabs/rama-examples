package rama.examples.testing;

import com.rpl.rama.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.RamaFunction0;
import com.rpl.rama.test.*;
import org.junit.jupiter.api.Test;

public class StreamTopologyWithMirrorTestingExample {
  public static class DepotModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));
    }
  }

  public static class CounterModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.clusterDepot("*mirror", DepotModule.class.getName(), "*depot");

      StreamTopology s = topologies.stream("counter");
      s.pstate("$$counts", PState.mapSchema(String.class, Long.class));
      s.source("*mirror").out("*k")
       .hashPartition("*k")
       .compoundAgg("$$counts", CompoundAgg.map("*k", Agg.count()));
    }
  }

  public void assertValueAttained(Object expected, RamaFunction0 f) throws Exception {
    long nanos = System.nanoTime();
    while(true) {
      Object val = f.invoke();
      if(expected.equals(val)) break;
      else if(System.nanoTime() - nanos > 1000000000L * 30) {
        throw new RuntimeException("Condition failed to attain " + expected + " != " + val);
      }
      Thread.sleep(50);
    }
  }

  @Test
  public void streamTopologyWithMirrorTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new DepotModule(), new LaunchConfig(8, 2));
      cluster.launchModule(new CounterModule(), new LaunchConfig(4, 2));

      Depot depot = cluster.clusterDepot(DepotModule.class.getName(), "*depot");
      PState counts = cluster.clusterPState(CounterModule.class.getName(), "$$counts");

      depot.append("cagney");
      depot.append("davis");
      depot.append("cagney");

      assertValueAttained(2L, () -> counts.selectOne(Path.key("cagney")));
      assertValueAttained(1L, () -> counts.selectOne(Path.key("davis")));
    }
  }
}
