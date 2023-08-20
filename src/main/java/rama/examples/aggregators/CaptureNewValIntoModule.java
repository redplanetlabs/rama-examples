package rama.examples.aggregators;

import com.rpl.rama.*;
import com.rpl.rama.module.MicrobatchTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import java.util.*;

public class CaptureNewValIntoModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    MicrobatchTopology mb = topologies.microbatch("mb");
    mb.pstate("$$p", PState.mapSchema(String.class,
                                      PState.mapSchema(String.class, List.class)));

    mb.source("*depot").out("*mb")
      .batchBlock(
        Block.explodeMicrobatch("*mb").out("*tuple")
             .each(Ops.EXPAND, "*tuple").out("*k1", "*k2", "*v")
             .hashPartition("*k1")
             .compoundAgg(
               "$$p",
               CompoundAgg.map(
                 "*k1",
                 CompoundAgg.map(
                   "*k2",
                   CompoundAgg.list(
                     Agg.count().captureNewValInto("*count"),
                     Agg.sum("*v").captureNewValInto("*sum")))))
             .each(Ops.PRINTLN, "Captured:", "*k1", "*k2", "*count", "*sum"));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new CaptureNewValIntoModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");

      System.out.println("Start");
      cluster.pauseMicrobatchTopology(moduleName, "mb");
      depot.append(Arrays.asList("a", "b", 3));
      depot.append(Arrays.asList("a", "c", 2));
      depot.append(Arrays.asList("d", "b", 9));
      depot.append(Arrays.asList("a", "b", 4));
      cluster.resumeMicrobatchTopology(moduleName, "mb");
      cluster.waitForMicrobatchProcessedCount(moduleName, "mb", 4);

      System.out.println("Second set of appends");
      cluster.pauseMicrobatchTopology(moduleName, "mb");
      depot.append(Arrays.asList("a", "b", 1));
      depot.append(Arrays.asList("f", "g", 11));
      cluster.resumeMicrobatchTopology(moduleName, "mb");
      cluster.waitForMicrobatchProcessedCount(moduleName, "mb", 6);
    }
  }
}
