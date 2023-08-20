package rama.examples.aggregators;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.InProcessCluster;
import com.rpl.rama.test.LaunchConfig;

import java.util.*;

public class TopNWordsModule implements RamaModule {
  private SubBatch wordCounts(String microbatchVar) {
    Block b = Block.explodeMicrobatch(microbatchVar).out("*word")
                   .hashPartition("*word")
                   .compoundAgg("$$wordCounts",
                                CompoundAgg.map(
                                  "*word",
                                  Agg.count().captureNewValInto("*count")));
    return new SubBatch(b, "*word", "*count");
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));

    MicrobatchTopology mb = topologies.microbatch("topWords");
    mb.pstate("$$wordCounts", PState.mapSchema(String.class, Long.class));
    mb.pstate("$$topWords", List.class).global();

    mb.source("*depot").out("*mb")
      .batchBlock(
        Block.subBatch(wordCounts("*mb")).out("*word", "*count")
             .each(Ops.PRINTLN, "Captured:", "*word", "*count")
             .each(Ops.TUPLE, "*word", "*count").out("*tuple")
             .globalPartition()
             .agg("$$topWords",
                  Agg.topMonotonic(3, "*tuple")
                     .idFunction(Ops.FIRST)
                     .sortValFunction(Ops.LAST)));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new TopNWordsModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState topWords = cluster.clusterPState(moduleName, "$$topWords");

      depot.append("apple");
      depot.append("orange");
      depot.append("strawberry");
      depot.append("papaya");
      depot.append("banana");
      depot.append("banana");
      depot.append("plum");
      depot.append("plum");
      depot.append("apple");
      depot.append("apple");
      depot.append("apple");
      depot.append("plum");

      cluster.waitForMicrobatchProcessedCount(moduleName, "topWords", 12);
      System.out.println("Top words: " + topWords.selectOne(Path.stay()));

      depot.append("orange");
      depot.append("orange");
      depot.append("orange");
      depot.append("apple");
      depot.append("orange");
      depot.append("orange");

      cluster.waitForMicrobatchProcessedCount(moduleName, "topWords", 18);
      System.out.println("Top words: " + topWords.selectOne(Path.stay()));
    }
  }
}
