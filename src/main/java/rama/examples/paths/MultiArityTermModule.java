package rama.examples.paths;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;

public class MultiArityTermModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*initDepot", Depot.random());
    setup.declareDepot("*termDepot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", Object.class).global();

    s.source("*initDepot")
     .localTransform("$$p", Path.key("a").termVal(1))
     .localTransform("$$p", Path.key("b").termVal(2));

    s.source("*termDepot")
     .localTransform("$$p", Path.key("a").term(Ops.PLUS, 10))
     .localTransform(
       "$$p",
       Path.key("b")
           .term((Integer v, Integer arg1, Integer arg2) -> {
             System.out.println("term function args: " + v + ", " + arg1 + ", " + arg2);
             return v * arg1 + arg2;
           }, 10, 20));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new MultiArityTermModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot initDepot = cluster.clusterDepot(moduleName, "*initDepot");
      Depot termDepot = cluster.clusterDepot(moduleName, "*termDepot");
      PState pstate = cluster.clusterPState(moduleName, "$$p");

      initDepot.append(null);
      System.out.println("Initial value: " + pstate.selectOne(Path.stay()));
      termDepot.append(null);
      System.out.println("After transform: " + pstate.selectOne(Path.stay()));
    }
  }
}
