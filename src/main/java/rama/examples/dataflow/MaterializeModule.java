package rama.examples.dataflow;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

public class MaterializeModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    MicrobatchTopology mb = topologies.microbatch("mb");
    mb.pstate("$$p1", Long.class).global().initialValue(0L);
    mb.pstate("$$p2", Long.class).global().initialValue(0L);
    mb.source("*depot").out("*microbatch")
      .batchBlock(
        Block.explodeMicrobatch("*microbatch").out("*v")
             .each(Ops.INC, "*v").out("*v2")
             .materialize("*v", "*v2").out("$$nums"))
      .batchBlock(
        Block.explodeMaterialized("$$nums").out("*v1", "*v2")
             .globalPartition()
             .agg("$$p1", Agg.sum("*v1")))
      .batchBlock(
        Block.explodeMaterialized("$$nums").out("*v1", "*v2")
             .globalPartition()
             .agg("$$p2", Agg.sum("*v2")));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new MaterializeModule();
      cluster.launchModule(module, new LaunchConfig(2, 2));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState p1 = cluster.clusterPState(moduleName, "$$p1");
      PState p2 = cluster.clusterPState(moduleName, "$$p2");

      depot.append(1);
      depot.append(2);
      depot.append(3);

      cluster.waitForMicrobatchProcessedCount(moduleName, "mb", 3);
      System.out.println("Vals: " + p1.selectOne(Path.stay()) + " " + p2.selectOne(Path.stay()));
    }
  }
}