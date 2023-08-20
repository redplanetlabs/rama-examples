package rama.examples.aggregators;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;

public class AggregateModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.pstate("$$count", Long.class).initialValue(0L);
    s.pstate("$$countByKey", PState.mapSchema(String.class, Long.class));
    s.source("*depot").out("*k")
     .agg("$$count", Agg.count())
     .compoundAgg("$$countByKey", CompoundAgg.map("*k", Agg.count()));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new AggregateModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState count = cluster.clusterPState(moduleName, "$$count");
      PState countByKey = cluster.clusterPState(moduleName, "$$countByKey");

      depot.append("james cagney");
      depot.append("bette davis");
      depot.append("spencer tracy");
      depot.append("james cagney");

      System.out.println("Count: " + count.selectOne(Path.stay()));
      System.out.println("Count by key: " + countByKey.select(Path.all()));
    }
  }
}
