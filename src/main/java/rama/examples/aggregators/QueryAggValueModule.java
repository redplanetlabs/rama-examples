package rama.examples.aggregators;

import com.rpl.rama.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import java.util.*;

public class QueryAggValueModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    topologies.query("q", "*nums").out("*res")
              .each(Ops.EXPLODE, "*nums").out("*num")
              .originPartition()
              .agg(Agg.sum("*num")).out("*res");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new QueryAggValueModule();
      cluster.launchModule(module, new LaunchConfig(2, 2));
      String moduleName = module.getClass().getName();

      QueryTopologyClient<Long> query = cluster.clusterQuery(moduleName, "q");

      System.out.println("Query 1: " + query.invoke(Arrays.asList(1, 2, 3)));
      System.out.println("Query 2: " + query.invoke(Arrays.asList(10, 15, 20, 25)));
    }
  }
}