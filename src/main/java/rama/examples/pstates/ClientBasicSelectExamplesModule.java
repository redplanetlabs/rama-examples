package rama.examples.pstates;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;
import java.util.Arrays;

public class ClientBasicSelectExamplesModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.hashBy(Ops.FIRST));

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p",
      PState.mapSchema(
        String.class,
        PState.setSchema(Long.class)));

    s.source("*depot").out("*tuple")
     .each(Ops.EXPAND, "*tuple").out("*k", "*v")
     .compoundAgg("$$p", CompoundAgg.map("*k", Agg.set("*v")));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new ClientBasicSelectExamplesModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState p = cluster.clusterPState(moduleName, "$$p");

      depot.append(Arrays.asList("cagney", 1));
      depot.append(Arrays.asList("cagney", 7));
      depot.append(Arrays.asList("cagney", 3));
      depot.append(Arrays.asList("cagney", 8));
      depot.append(Arrays.asList("davis", 10));
      depot.append(Arrays.asList("davis", 12));
      depot.append(Arrays.asList("davis", 14));

      System.out.println(
        "select (davis): " +  p.select(Path.key("davis")));
      System.out.println("selectOne (davis): " + p.selectOne(Path.key("davis")));
      System.out.println(
        "select (cagney): " +
          p.select(Path.key("cagney").all().filterPred(Ops.IS_ODD)));
    }
  }
}
