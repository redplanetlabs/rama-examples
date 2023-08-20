package rama.examples.stream;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

import java.util.Arrays;

public class BasicStreamTopologyModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.hashBy(Ops.FIRST));
    setup.declareDepot("*depot2", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p1", PState.mapSchema(String.class, Long.class));
    s.pstate(
      "$$p2",
      PState.mapSchema(
        String.class,
        PState.mapSchema(String.class, Long.class).subindexed()));

    s.source("*depot").out("*tuple")
     .each(Ops.EXPAND, "*tuple").out("*k1", "*k2")
     .compoundAgg("$$p1", CompoundAgg.map("*k1", Agg.count()))
     .compoundAgg("$$p2", CompoundAgg.map("*k1", CompoundAgg.map("*k2", Agg.count())))
     .ifTrue(new Expr(Ops.NOT_EQUAL, "*k1", "*k2"),
       Block.hashPartition("*k2")
            .compoundAgg("$$p1", CompoundAgg.map("*k2", Agg.count()))
            .compoundAgg("$$p2", CompoundAgg.map("*k2", CompoundAgg.map("*k1", Agg.count()))));

    s.source("*depot2").out("*v")
     .each(Ops.CURRENT_TASK_ID).out("*taskId")
     .each(Ops.PRINTLN, "From *depot2:", "*taskId", "*v");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new BasicStreamTopologyModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      Depot depot2 = cluster.clusterDepot(moduleName, "*depot2");
      PState p1 = cluster.clusterPState(moduleName, "$$p1");
      PState p2 = cluster.clusterPState(moduleName, "$$p2");

      depot.append(Arrays.asList("a", "b"));
      depot.append(Arrays.asList("b", "c"));
      depot.append(Arrays.asList("a", "a"));

      System.out.println("a count: " + p1.selectOne(Path.key("a")));
      System.out.println("b count: " + p1.selectOne(Path.key("b")));
      System.out.println("c count: " + p1.selectOne(Path.key("c")));

      System.out.println("b subcounts: " + p2.select(Path.key("b").all()));

      depot2.append("X");
      depot2.append("Y");
    }
  }
}