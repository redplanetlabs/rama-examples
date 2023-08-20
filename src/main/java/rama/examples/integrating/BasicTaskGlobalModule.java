package rama.examples.integrating;

import com.rpl.rama.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;

public class BasicTaskGlobalModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareObject("*globalValue", 7);
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.source("*depot")
     .each(Ops.PRINTLN, "Task", new Expr(Ops.CURRENT_TASK_ID), "->", "*globalValue")
     .shufflePartition()
     .each(Ops.PRINTLN, "Task", new Expr(Ops.CURRENT_TASK_ID), "->", "*globalValue");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new BasicTaskGlobalModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      depot.append(null);
    }
  }
}
