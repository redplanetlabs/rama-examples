package rama.examples.microbatch;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

import java.util.Arrays;

public class ExampleMicrobatchTopologyModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*keyPairsDepot", Depot.hashBy(Ops.FIRST));
    setup.declareDepot("*numbersDepot", Depot.random());

    MicrobatchTopology mb = topologies.microbatch("mb");
    mb.pstate(
       "$$keyPairCounts",
       PState.mapSchema(
         String.class,
         PState.mapSchema(String.class, Long.class).subindexed()));
    mb.pstate("$$globalSum", Long.class).global().initialValue(0L);

    mb.source("*keyPairsDepot").out("*microbatch")
      .explodeMicrobatch("*microbatch").out("*tuple")
      .each(Ops.EXPAND, "*tuple").out("*k", "*k2")
      .compoundAgg("$$keyPairCounts", CompoundAgg.map("*k", CompoundAgg.map("*k2", Agg.count())));

    mb.source("*numbersDepot").out("*microbatch")
      .batchBlock(
        Block.explodeMicrobatch("*microbatch").out("*v")
             .globalPartition()
             .agg("$$globalSum", Agg.sum("*v")));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new ExampleMicrobatchTopologyModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot keyPairsDepot = cluster.clusterDepot(moduleName, "*keyPairsDepot");
      Depot numbersDepot = cluster.clusterDepot(moduleName, "*numbersDepot");
      PState keyPairCounts = cluster.clusterPState(moduleName, "$$keyPairCounts");
      PState globalSum = cluster.clusterPState(moduleName, "$$globalSum");

      numbersDepot.append(1);
      numbersDepot.append(3);
      numbersDepot.append(7);

      keyPairsDepot.append(Arrays.asList("a", "b"));
      keyPairsDepot.append(Arrays.asList("a", "b"));
      keyPairsDepot.append(Arrays.asList("a", "c"));
      keyPairsDepot.append(Arrays.asList("x", "y"));
      keyPairsDepot.append(Arrays.asList("x", "y"));
      keyPairsDepot.append(Arrays.asList("x", "y"));
      keyPairsDepot.append(Arrays.asList("x", "z"));

      cluster.waitForMicrobatchProcessedCount(moduleName, "mb", 10);

      System.out.println("Global sum: " + globalSum.selectOne(Path.stay()));
      System.out.println("Counts for 'a': " + keyPairCounts.select(Path.key("a").all()));
      System.out.println("Counts for 'x': " + keyPairCounts.select(Path.key("x").all()));
    }
  }
}