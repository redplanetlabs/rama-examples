package rama.examples.stream;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

public class SumAckReturnModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));

    StreamTopology s = topologies.stream("sumTopology");

    s.source("*depot", StreamSourceOptions.ackReturnAgg(Agg::sum)).out("*v")
     .each(Ops.RANGE, 0, "*v")
     .shufflePartition()
     .ackReturn("*v");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new SumAckReturnModule();
      cluster.launchModule(module, new LaunchConfig(8, 3));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");

      System.out.println("Ack return for 3: " + depot.append(3));
      System.out.println("Ack return for 5: " + depot.append(5));
      System.out.println("Ack return for 10: " + depot.append(10));
    }
  }
}
