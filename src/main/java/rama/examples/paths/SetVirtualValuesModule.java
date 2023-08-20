package rama.examples.paths;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;

public class SetVirtualValuesModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", Object.class).global();

    s.source("*depot").out("*v")
     .localTransform("$$p", Path.voidSetElem().termVal("*v"));

  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new SetVirtualValuesModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState pstate = cluster.clusterPState(moduleName, "$$p");

      depot.append("a");
      System.out.println("After first transform: " + pstate.select(Path.all()));
      depot.append("f");
      System.out.println("After second transform: " + pstate.select(Path.all()));
      depot.append("c");
      System.out.println("After third transform: " + pstate.select(Path.all()));
    }
  }
}
