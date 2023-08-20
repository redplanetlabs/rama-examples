package rama.examples.aggregators;

import com.rpl.rama.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import java.util.*;

public class GroupByModule implements RamaModule {
  private SubBatch aggregatedTuples(String tuplesVar) {
    Block b = Block.each(Ops.EXPLODE, tuplesVar).out("*tuple")
                   .each(Ops.EXPAND, "*tuple").out("*k", "*val")
                   .groupBy("*k",
                     Block.agg(Agg.count()).out("*count")
                          .agg(Agg.sum("*val")).out("*sum"));
    return new SubBatch(b, "*k", "*count", "*sum");
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    topologies.query("q", "*tuples").out("*topTwoTuples")
              .subBatch(aggregatedTuples("*tuples")).out("*k", "*count", "*sum")
              .each(Ops.TUPLE, "*k", "*count", "*sum").out("*tuple")
              .originPartition()
              .agg(Agg.topMonotonic(2, "*tuple")
                      .idFunction(Ops.FIRST)
                      .sortValFunction(Ops.LAST)).out("*topTwoTuples");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new GroupByModule();
      cluster.launchModule(module, new LaunchConfig(2, 2));
      String moduleName = module.getClass().getName();

      QueryTopologyClient<Long> query = cluster.clusterQuery(moduleName, "q");

      System.out.println(
        "Query: " +
        query.invoke(
          Arrays.asList(
            Arrays.asList("apple", 1),
            Arrays.asList("banana", 9),
            Arrays.asList("apple", 6),
            Arrays.asList("plum", 1),
            Arrays.asList("plum", 1),
            Arrays.asList("plum", 1))));
    }
  }
}