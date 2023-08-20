package rama.examples.paths;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;

public class TermVoidModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*initDepot", Depot.random());
    setup.declareDepot("*termVoidDepot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", Object.class).global();

    s.source("*initDepot")
     .localTransform("$$p", Path.key("a").afterElem().termVal(1))
     .localTransform("$$p", Path.key("a").afterElem().termVal(2))
     .localTransform("$$p", Path.key("a").afterElem().termVal(3))
     .localTransform("$$p", Path.key("b").termVal("xyz"));

    s.source("*termVoidDepot")
     .localTransform("$$p", Path.key("a").nth(1).termVoid())
     .localTransform("$$p", Path.key("b").termVoid());
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new TermVoidModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot initDepot = cluster.clusterDepot(moduleName, "*initDepot");
      Depot termDepot = cluster.clusterDepot(moduleName, "*termVoidDepot");
      PState pstate = cluster.clusterPState(moduleName, "$$p");

      initDepot.append(null);
      System.out.println("Initial value: " + pstate.selectOne(Path.stay()));
      termDepot.append(null);
      System.out.println("After transform: " + pstate.selectOne(Path.stay()));
    }
  }
}
