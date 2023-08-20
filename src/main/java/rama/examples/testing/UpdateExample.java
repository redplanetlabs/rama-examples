package rama.examples.testing;

import com.rpl.rama.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class UpdateExample {
  public static class CounterModule_v1 implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));

      StreamTopology s = topologies.stream("counter");
      s.pstate("$$counts", PState.mapSchema(String.class, Long.class));
      s.source("*depot").out("*k")
       .compoundAgg("$$counts", CompoundAgg.map("*k", Agg.sum(2)));
    }

    @Override
    public String getModuleName() {
      return "CounterModule";
    }
  }

  public static class CounterModule_v2 implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));

      StreamTopology s = topologies.stream("counter");
      s.pstate("$$counts", PState.mapSchema(String.class, Long.class));
      s.source("*depot").out("*k")
       .compoundAgg("$$counts", CompoundAgg.map("*k", Agg.count()));
    }

    @Override
    public String getModuleName() {
      return "CounterModule";
    }
  }

  @Test
  public void updateTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new CounterModule_v1(), new LaunchConfig(4, 2));
      Depot depot = cluster.clusterDepot("CounterModule", "*depot");
      PState counts = cluster.clusterPState("CounterModule", "$$counts");

      depot.append("cagney");
      assertEquals(2, (Long) counts.selectOne(Path.key("cagney")));

      cluster.updateModule(new CounterModule_v2());

      depot.append("cagney");
      assertEquals(3, (Long) counts.selectOne(Path.key("cagney")));
    }
  }
}
