package rama.examples.mirrors;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;

public class UsingMirrorsExample {
  public static class Module1 implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot1", Depot.hashBy(Ops.IDENTITY));
      setup.declareDepot("*depot2", Depot.hashBy(Ops.IDENTITY));

      StreamTopology s = topologies.stream("s");
      s.pstate("$$p", PState.mapSchema(String.class, Long.class));
      s.source("*depot1").out("*k")
       .compoundAgg("$$p", CompoundAgg.map("*k", Agg.count()));

      topologies.query("qq", "*v1", "*v2").out("*res")
                .each(Ops.TIMES, new Expr(Ops.INC, "*v1"), "*v2").out("*res")
                .originPartition();
    }
  }

  public static class Module2 implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.clusterDepot("*depot2", Module1.class.getName(), "*depot2");
      setup.clusterPState("$$mirror", Module1.class.getName(), "$$p");
      setup.clusterQuery("*mirrorQuery", Module1.class.getName(), "qq");

      StreamTopology s = topologies.stream("s");
      s.source("*depot2", StreamSourceOptions.startFromBeginning()).out("*k")
       .select("$$mirror", Path.key("*k")).out("*count")
       .invokeQuery("*mirrorQuery", 3, 7).out("*queryResult")
       .each(Ops.PRINTLN, "Results:", "*count", "*queryResult");
    }
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module1 = new Module1();
      cluster.launchModule(module1, new LaunchConfig(4, 4));
      String module1Name = module1.getClass().getName();
      cluster.launchModule(new Module2(), new LaunchConfig(2, 2));

      Depot depot1 = cluster.clusterDepot(module1Name, "*depot1");
      Depot depot2 = cluster.clusterDepot(module1Name, "*depot2");

      depot1.append("a");
      depot1.append("a");
      depot1.append("b");
      depot2.append("a");
      depot2.append("b");
      depot2.append("c");

      Thread.sleep(2000);
    }
  }
}