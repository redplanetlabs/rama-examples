package rama.examples.paths;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import java.util.*;

public class FilteredListModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*initDepot", Depot.random());
    setup.declareDepot("*filteredListDepot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", Object.class).global();

    s.source("*initDepot")
     .localTransform("$$p", Path.afterElem().termVal(0))
     .localTransform("$$p", Path.afterElem().termVal(1))
     .localTransform("$$p", Path.afterElem().termVal(2))
     .localTransform("$$p", Path.afterElem().termVal(3))
     .localTransform("$$p", Path.afterElem().termVal(4))
     .localTransform("$$p", Path.afterElem().termVal(5));

    s.source("*filteredListDepot")
     .localTransform(
       "$$p",
       Path.filteredList(Path.filterPred(Ops.IS_EVEN))
           .term((List l) -> {
             List ret = new ArrayList(l);
             Collections.reverse(ret);
             return ret;
           } ));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new FilteredListModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot initDepot = cluster.clusterDepot(moduleName, "*initDepot");
      Depot filteredListDepot = cluster.clusterDepot(moduleName, "*filteredListDepot");
      PState pstate = cluster.clusterPState(moduleName, "$$p");

      initDepot.append(null);
      System.out.println("Initial list: " + pstate.select(Path.all()));
      filteredListDepot.append(null);
      System.out.println("After transform: " + pstate.select(Path.all()));
    }
  }
}
