package rama.examples.pstates;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;
import java.util.*;

public class BasicReactiveModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.hashBy(Ops.FIRST));

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", PState.mapSchema(String.class, PState.mapSchema(String.class, Long.class)));
    s.source("*depot").out("*tuple")
     .each(Ops.EXPAND, "*tuple").out("*k1", "*k2")
     .compoundAgg("$$p", CompoundAgg.map("*k1", CompoundAgg.map("*k2", Agg.count())));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new BasicReactiveModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState p = cluster.clusterPState(moduleName, "$$p");

      depot.append(Arrays.asList("a", "b"));
      depot.append(Arrays.asList("a", "c"));

      ProxyState<Map> proxy = p.proxy(Path.key("a"));
      System.out.println("Initial value: " + proxy.get());

      depot.append(Arrays.asList("a", "d"));
      Thread.sleep(50);
      System.out.println("New value: " + proxy.get());

      depot.append(Arrays.asList("a", "c"));
      Thread.sleep(50);
      System.out.println("New value: " + proxy.get());
    }
  }
}
