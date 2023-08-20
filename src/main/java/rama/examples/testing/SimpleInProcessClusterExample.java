package rama.examples.testing;

import com.rpl.rama.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleInProcessClusterExample {
  public static class SimpleModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));

      StreamTopology s = topologies.stream("counter");
      s.pstate("$$counts", PState.mapSchema(String.class, Long.class));
      s.source("*depot").out("*k")
       .compoundAgg("$$counts", CompoundAgg.map("*k", Agg.count()));
    }
  }

  @Test
  public void simpleTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new SimpleModule(), new LaunchConfig(4, 2));

      String moduleName = SimpleModule.class.getName();
      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState counts = cluster.clusterPState(moduleName, "$$counts");

      depot.append("cagney");
      depot.append("davis");
      depot.append("cagney");

      assertEquals(2, (Long) counts.selectOne(Path.key("cagney")));
      assertEquals(1, (Long) counts.selectOne(Path.key("davis")));
      assertNull(counts.selectOne(Path.key("garbo")));
    }
  }
}
