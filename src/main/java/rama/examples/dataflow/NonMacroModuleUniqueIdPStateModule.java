package rama.examples.dataflow;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

public class NonMacroModuleUniqueIdPStateModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    NonMacroModuleUniqueIdPState id = new NonMacroModuleUniqueIdPState("$$id");
    id.declarePState(s);
    Block.Impl b = s.source("*depot");
    b = id.genId(b, "*id");
    b.each(Ops.PRINTLN, "New ID:", "*id");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new NonMacroModuleUniqueIdPStateModule();
      cluster.launchModule(module, new LaunchConfig(4, 2));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");

      depot.append(null);
      depot.append(null);
      depot.append(null);
      depot.append(null);
    }
  }
}