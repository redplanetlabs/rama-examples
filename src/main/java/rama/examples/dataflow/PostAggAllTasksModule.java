package rama.examples.dataflow;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

public class PostAggAllTasksModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    MicrobatchTopology mb = topologies.microbatch("mb");
    mb.pstate("$$p", PState.mapSchema(String.class, Long.class));
    mb.source("*depot").out("*microbatch")
      .each(Ops.PRINTLN, "Beginning microbatch")
      .batchBlock(
        Block.explodeMicrobatch("*microbatch").out("*k")
             .each(Ops.PRINTLN, "Pre-agg:", "*k")
             .hashPartition("*k")
             .compoundAgg("$$p", CompoundAgg.map("*k", Agg.count()))
             .localSelect("$$p", Path.stay()).out("*m")
             .each(Ops.PRINTLN, "Post-agg:", "*m"));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new PostAggAllTasksModule();
      cluster.launchModule(module, new LaunchConfig(2, 2));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");

      depot.append("a");
      depot.append("b");
      depot.append("a");
      depot.append("g");
      depot.append("d");
      depot.append("d");

      cluster.waitForMicrobatchProcessedCount(moduleName, "mb", 6);
    }
  }
}