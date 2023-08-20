package rama.examples.partitioners;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;

public class BasicPartitionerExampleModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.source("*depot").out("*k")
     .each(Ops.PRINTLN, "Start task", "*k", new Expr(Ops.CURRENT_TASK_ID))
     .hashPartition("*k")
     .each(Ops.PRINTLN, "End task", "*k", new Expr(Ops.CURRENT_TASK_ID));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new BasicPartitionerExampleModule();
      cluster.launchModule(module, new LaunchConfig(8, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");

      depot.append("cagney");
      depot.append("cagney");
      depot.append("lemmon");
      depot.append("lemmon");
      depot.append("bergman");
      depot.append("bergman");
    }
  }
}
