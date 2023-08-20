package rama.examples.paths;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import java.util.*;

public class SubselectModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*initDepot", Depot.random());
    setup.declareDepot("*subselectDepot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", Object.class).global();

    s.source("*initDepot")
     .localTransform("$$p", Path.afterElem().termVal(null))
     .localTransform("$$p", Path.last().afterElem().termVal(1))
     .localTransform("$$p", Path.last().afterElem().termVal(2))
     .localTransform("$$p", Path.last().afterElem().termVal(3))
     .localTransform("$$p", Path.last().afterElem().termVal(4))
     .localTransform("$$p", Path.afterElem().termVal(null))
     .localTransform("$$p", Path.last().afterElem().termVal(5))
     .localTransform("$$p", Path.last().afterElem().termVal(6))
     .localTransform("$$p", Path.last().afterElem().termVal(7))
     .localTransform("$$p", Path.afterElem().termVal(null))
     .localTransform("$$p", Path.last().afterElem().termVal(8))
     .localTransform("$$p", Path.last().afterElem().termVal(9))
     .localTransform("$$p", Path.last().afterElem().termVal(10));

    s.source("*subselectDepot")
     .localTransform(
       "$$p",
       Path.subselect(Path.all().all().filterPred(Ops.IS_ODD))
           .term((List l) -> {
             List ret = new ArrayList(l);
             Collections.reverse(ret);
             return ret;
           }));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new SubselectModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot initDepot = cluster.clusterDepot(moduleName, "*initDepot");
      Depot subselectDepot = cluster.clusterDepot(moduleName, "*subselectDepot");
      PState pstate = cluster.clusterPState(moduleName, "$$p");

      initDepot.append(null);
      System.out.println("Initial contents: " + pstate.selectOne(Path.stay()));
      subselectDepot.append(null);
      System.out.println("After transform: " + pstate.selectOne(Path.stay()));
    }
  }
}
