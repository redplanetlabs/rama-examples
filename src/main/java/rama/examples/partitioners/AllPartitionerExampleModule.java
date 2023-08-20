package rama.examples.partitioners;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;

public class AllPartitionerExampleModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.source("*depot").out("*k")
     .each(Ops.PRINTLN, "Start task", "*k", new Expr(Ops.CURRENT_TASK_ID))
     .allPartition()
     .each(Ops.PRINTLN, "End task", "*k", new Expr(Ops.CURRENT_TASK_ID));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new AllPartitionerExampleModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");

      depot.append("groucho");
      depot.append("harpo");
      depot.append("chico");
    }
  }
}
