package rama.examples.aggregators;

import com.rpl.rama.*;
import com.rpl.rama.ops.LimitAgg;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import java.util.*;

public class LimitWithOptionsModule implements RamaModule {
  private SubBatch limitTuples(String tuplesVar) {
    Block b = Block.each(Ops.EXPLODE, tuplesVar).out("*tuple")
                   .each(Ops.EXPAND, "*tuple").out("*v1", "*v2", "*v3")
                   .globalPartition()
                   .limitAgg(LimitAgg.create(3, "*v1", "*v3")
                                     .sort("*v2")
                                     .reverse()
                                     .indexVar("*index"))
                   .each(Ops.PRINTLN, "Post agg data:", "*index", "*v1", "*v3");
    return new SubBatch(b, "*v1", "*v3");
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    topologies.query("q", "*tuples").out("*res")
              .subBatch(limitTuples("*tuples")).out("*x", "*y")
              .originPartition()
              .agg(Agg.sum("*x")).out("*res1")
              .agg(Agg.sum("*y")).out("*res2")
              .each(Ops.TUPLE, "*res1", "*res2").out("*res");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new LimitWithOptionsModule();
      cluster.launchModule(module, new LaunchConfig(2, 2));
      String moduleName = module.getClass().getName();

      QueryTopologyClient<List> query = cluster.clusterQuery(moduleName, "q");

      System.out.println(
        "Query: " +
          query.invoke(
            Arrays.asList(
              Arrays.asList(1, 2, 3),
              Arrays.asList(10, 11, 12),
              Arrays.asList(6, 4, 5),
              Arrays.asList(1000, 1000, 1000))));
    }
  }
}