package rama.examples.paths;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;

public class MapKeyModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*initDepot", Depot.random());
    setup.declareDepot("*mapKeyDepot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", Object.class).global();

    s.source("*initDepot")
     .localTransform("$$p", Path.key("a").termVal(0))
     .localTransform("$$p", Path.key("b").termVal(1));

    s.source("*mapKeyDepot")
     .localTransform("$$p", Path.mapKey("a").termVal("c"));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new MapKeyModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot initDepot = cluster.clusterDepot(moduleName, "*initDepot");
      Depot mapKeyDepot = cluster.clusterDepot(moduleName, "*mapKeyDepot");
      PState pstate = cluster.clusterPState(moduleName, "$$p");

      initDepot.append(null);
      System.out.println("Init: " + pstate.select(Path.all()));
      mapKeyDepot.append(null);
      System.out.println("After transform: " + pstate.select(Path.all()));
    }
  }
}
