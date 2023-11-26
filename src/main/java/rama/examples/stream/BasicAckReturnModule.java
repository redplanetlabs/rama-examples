package rama.examples.stream;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

public class BasicAckReturnModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", PState.mapSchema(String.class, Long.class));

    s.source("*depot").out("*k")
     .compoundAgg("$$p", CompoundAgg.map("*k", Agg.count()))
     .localSelect("$$p", Path.key("*k")).out("*v")
     .ackReturn("*v");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new BasicAckReturnModule();
      cluster.launchModule(module, new LaunchConfig(4, 2));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");

      System.out.println("Ack return 1: " + depot.append("a"));
      System.out.println("Ack return 2: " + depot.append("a"));
      System.out.println("Ack return 3: " + depot.append("a"));
    }
  }
}
