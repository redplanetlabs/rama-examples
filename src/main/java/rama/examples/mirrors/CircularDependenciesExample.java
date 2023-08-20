package rama.examples.mirrors;

import com.rpl.rama.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;

public class CircularDependenciesExample {
  public static class ModuleA_v1 implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());
    }

    @Override
    public String getModuleName() {
      return "ModuleA";
    }
  }

  public static class ModuleB implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.clusterDepot("*depot", "ModuleA", "*depot");

      StreamTopology s = topologies.stream("s");
      s.pstate("$$p", PState.mapSchema(String.class, Long.class));
      s.source("*depot").out("*k")
        .hashPartition("*k")
        .compoundAgg("$$p", CompoundAgg.map("*k", Agg.count()));
    }
  }

  public static class ModuleA_v2 implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());
      setup.declareDepot("*depot2", Depot.random());
      setup.clusterPState("$$mirror", ModuleB.class.getName(), "$$p");

      StreamTopology s = topologies.stream("s");
      s.source("*depot2").out("*k")
        .select("$$mirror", Path.key("*k")).out("*count")
        .each(Ops.PRINTLN, "Mirror count:", "*k", "*count");
    }

    @Override
    public String getModuleName() {
      return "ModuleA";
    }
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new ModuleA_v1(), new LaunchConfig(2, 2));

      RamaModule moduleB = new ModuleB();
      cluster.launchModule(moduleB, new LaunchConfig(4, 4));
      String moduleBName = moduleB.getClass().getName();

      cluster.updateModule(new ModuleA_v2());

      Depot depot = cluster.clusterDepot("ModuleA", "*depot");
      Depot depot2 = cluster.clusterDepot("ModuleA", "*depot2");

      depot.append("a");
      depot.append("a");
      depot.append("b");

      Thread.sleep(2000);

      depot2.append("a");
      depot2.append("b");
    }
  }
}
