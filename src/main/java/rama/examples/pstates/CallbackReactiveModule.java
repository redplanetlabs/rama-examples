package rama.examples.pstates;

import com.rpl.rama.*;
import com.rpl.rama.diffs.Diff;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;
import java.util.*;

public class CallbackReactiveModule implements RamaModule {
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
      RamaModule module = new CallbackReactiveModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState p = cluster.clusterPState(moduleName, "$$p");

      depot.append(Arrays.asList("a", "b"));
      depot.append(Arrays.asList("a", "c"));

      ProxyState<Map> proxy = p.proxy(Path.key("a"), new ProxyState.Callback<Map>() {
        @Override
        public void change(Map newVal, Diff diff, Map oldVal) {
          System.out.println("Received callback: " + newVal + ", " + diff + ", " + oldVal);
        }
      });
      depot.append(Arrays.asList("a", "d"));
      depot.append(Arrays.asList("a", "c"));

      Thread.sleep(50);
    }
  }
}
