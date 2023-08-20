package rama.examples.paths;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import java.util.*;

public class SubListModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*initDepot", Depot.random());
    setup.declareDepot("*subListDepot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", Object.class).global();

    s.source("*initDepot")
     .localTransform("$$p", Path.key("k").afterElem().termVal("a"))
     .localTransform("$$p", Path.key("k").afterElem().termVal("b"))
     .localTransform("$$p", Path.key("k").afterElem().termVal("c"))
     .localTransform("$$p", Path.key("k").afterElem().termVal("d"))
     .localTransform("$$p", Path.key("k").afterElem().termVal("e"))
     .localTransform("$$p", Path.key("k").afterElem().termVal("f"));

    s.source("*subListDepot")
     .localTransform(
       "$$p",
       Path.key("k")
           .sublist(1, 5)
           .term((List l) -> {
             List ret = new ArrayList(l);
             Collections.reverse(ret);
             return ret;
           } ));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new SubListModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot initDepot = cluster.clusterDepot(moduleName, "*initDepot");
      Depot subListDepot = cluster.clusterDepot(moduleName, "*subListDepot");
      PState pstate = cluster.clusterPState(moduleName, "$$p");

      initDepot.append(null);
      System.out.println("Initial nested list: " + pstate.selectOne(Path.key("k")));
      subListDepot.append(null);
      System.out.println("After transform: " + pstate.selectOne(Path.key("k")));
    }
  }
}
