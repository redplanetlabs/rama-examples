package rama.examples.partitioners;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;
import java.util.*;

public class MirrorPStateExample {
  public static class Module1 implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));

      StreamTopology s = topologies.stream("s");
      s.pstate("$$p", PState.mapSchema(String.class, Long.class));

      s.source("*depot").out("*k")
       .compoundAgg("$$p", CompoundAgg.map("*k", Agg.count()));
    }
  }

  public static class Module2 implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
      setup.declareDepot("*depot", Depot.random());
      setup.clusterPState("$$other", Module1.class.getName(), "$$p");

      StreamTopology s = topologies.stream("s");
      s.pstate("$$p", PState.mapSchema(String.class, Long.class));

      s.source("*depot").out("*tuple")
       .each(Ops.EXPAND, "*tuple").out("*k", "*v")
       .hashPartition("$$other", "*k")
       .localSelect("$$other", Path.key("*k").nullToVal(0L)).out("*n")
       .each(Ops.PLUS_LONG, "*v", "*n").out("*newv")
       .hashPartition("*k")
       .localTransform("$$p", Path.key("*k").termVal("*newv"));
    }
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module1 = new Module1();
      cluster.launchModule(module1, new LaunchConfig(8, 4));
      RamaModule module2 = new Module2();
      cluster.launchModule(module2, new LaunchConfig(4, 4));
      String module1Name = module1.getClass().getName();
      String module2Name = module2.getClass().getName();

      Depot depot1 = cluster.clusterDepot(module1Name, "*depot");
      Depot depot2 = cluster.clusterDepot(module2Name, "*depot");
      PState pstate = cluster.clusterPState(module2Name, "$$p");

      depot1.append("a");
      depot1.append("a");

      depot2.append(Arrays.asList("a", 10));
      System.out.println("Val: " + pstate.selectOne(Path.key("a")));
    }
  }
}
