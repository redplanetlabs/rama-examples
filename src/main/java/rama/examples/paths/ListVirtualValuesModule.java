package rama.examples.paths;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;

public class ListVirtualValuesModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*appendDepot", Depot.random());
    setup.declareDepot("*prependDepot", Depot.random());
    setup.declareDepot("*insertDepot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", Object.class).global();

    s.source("*appendDepot").out("*v")
     .localTransform("$$p", Path.afterElem().termVal("*v"));

    s.source("*prependDepot").out("*v")
     .localTransform("$$p", Path.beforeElem().termVal("*v"));

    s.source("*insertDepot").out("*v")
     .localTransform("$$p", Path.beforeIndex(2).termVal("*v"));

  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new ListVirtualValuesModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot appendDepot = cluster.clusterDepot(moduleName, "*appendDepot");
      Depot prependDepot = cluster.clusterDepot(moduleName, "*prependDepot");
      Depot insertDepot = cluster.clusterDepot(moduleName, "*insertDepot");
      PState pstate = cluster.clusterPState(moduleName, "$$p");

      appendDepot.append("a");
      appendDepot.append("b");
      appendDepot.append("c");
      System.out.println("After appends: " + pstate.select(Path.all()));
      prependDepot.append("d");
      System.out.println("After prepend: " + pstate.select(Path.all()));
      insertDepot.append("e");
      System.out.println("After insert: " + pstate.select(Path.all()));
    }
  }
}
