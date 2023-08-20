package rama.examples.testing;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MicrobatchTestingExample {
  public static class MicrobatchModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));

      MicrobatchTopology mb = topologies.microbatch("counter");
      mb.pstate("$$counts", PState.mapSchema(String.class, Long.class));
      mb.source("*depot").out("*mb")
        .explodeMicrobatch("*mb").out("*k")
        .compoundAgg("$$counts", CompoundAgg.map("*k", Agg.count()));
    }
  }

  @Test
  public void microbatchTest() throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new MicrobatchModule(), new LaunchConfig(4, 2));

      String moduleName = MicrobatchModule.class.getName();
      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState counts = cluster.clusterPState(moduleName, "$$counts");

      depot.append("cagney");
      depot.append("davis");
      depot.append("cagney");

      cluster.waitForMicrobatchProcessedCount(moduleName, "counter", 3);
      assertEquals(2, (Long) counts.selectOne(Path.key("cagney")));
      assertEquals(1, (Long) counts.selectOne(Path.key("davis")));
      assertNull(counts.selectOne(Path.key("garbo")));

      depot.append("cagney");

      cluster.waitForMicrobatchProcessedCount(moduleName, "counter", 4);
      assertEquals(3, (Long) counts.selectOne(Path.key("cagney")));
    }
  }
}
