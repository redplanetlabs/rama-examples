package rama.examples.aggregators;

import com.rpl.rama.*;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;

public class TmpPStateModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    MicrobatchTopology mb = topologies.microbatch("mb");
    mb.pstate("$$maxes", PState.mapSchema(String.class, Long.class));
    mb.pstate("$$mins", PState.mapSchema(String.class, Long.class));

    mb.source("*depot").out("*mb")
      .batchBlock(
        Block.explodeMicrobatch("*mb").out("*k")
             .hashPartition("*k")
             .compoundAgg(CompoundAgg.map("*k", Agg.count())).out("$$keyCounts"))
      .batchBlock(
        Block.allPartition()
             .localSelect("$$keyCounts", Path.all()).out("*tuple")
             .each(Ops.EXPAND, "*tuple").out("*k", "*count")
             .hashPartition("*k")
             .compoundAgg("$$maxes", CompoundAgg.map("*k", Agg.max("*count"))))
      .batchBlock(
        Block.allPartition()
             .localSelect("$$keyCounts", Path.all()).out("*tuple")
             .each(Ops.EXPAND, "*tuple").out("*k", "*count")
             .hashPartition("*k")
             .compoundAgg("$$mins", CompoundAgg.map("*k", Agg.min("*count"))));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new TmpPStateModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState maxes = cluster.clusterPState(moduleName, "$$maxes");
      PState mins = cluster.clusterPState(moduleName, "$$mins");

      System.out.println("Start");
      cluster.pauseMicrobatchTopology(moduleName, "mb");
      depot.append("apple");
      depot.append("apple");
      depot.append("plum");
      cluster.resumeMicrobatchTopology(moduleName, "mb");
      cluster.waitForMicrobatchProcessedCount(moduleName, "mb", 3);

      System.out.println("apple max: " + maxes.selectOne(Path.key("apple")));
      System.out.println("apple min: " + mins.selectOne(Path.key("apple")));

      System.out.println("Second set of appends");
      cluster.pauseMicrobatchTopology(moduleName, "mb");
      depot.append("apple");
      depot.append("banana");
      depot.append("banana");
      cluster.resumeMicrobatchTopology(moduleName, "mb");
      cluster.waitForMicrobatchProcessedCount(moduleName, "mb", 6);

      System.out.println("apple max: " + maxes.selectOne(Path.key("apple")));
      System.out.println("apple min: " + mins.selectOne(Path.key("apple")));
    }
  }
}