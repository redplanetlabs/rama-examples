package rama.examples.dataflow;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

public class GlobalSumModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    MicrobatchTopology mb = topologies.microbatch("mb");
    mb.pstate("$$sum", Long.class).global().initialValue(0L);
    mb.source("*depot").out("*microbatch")
     .each(Ops.PRINTLN, "Beginning microbatch")
     .batchBlock(
       Block.explodeMicrobatch("*microbatch").out("*v")
            .each(Ops.PRINTLN, "Pre-agg:", "*v")
            .globalPartition()
            .agg("$$sum", Agg.sum("*v"))
            .localSelect("$$sum", Path.stay()).out("*sum")
            .each(Ops.PRINTLN, "Post-agg:", "*sum"));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new GlobalSumModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState sum = cluster.clusterPState(moduleName, "$$sum");

      depot.append(1);
      depot.append(2);
      depot.append(3);
      depot.append(4);

      cluster.waitForMicrobatchProcessedCount(moduleName, "mb", 4);
      System.out.println("Sum: " + sum.selectOne(Path.stay()));
    }
  }
}