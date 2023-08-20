package rama.examples.dataflow;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

import java.util.Arrays;

public class AtomicBlockModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.source("*depot").out("*list")
     .atomicBlock(
       Block.each(Ops.EXPLODE, "*list").out("*v")
            .each(Ops.PRINTLN, "A:", "*v")
            .shufflePartition()
            .each(Ops.PRINTLN, "B:", "*v"))
     .each(Ops.PRINTLN, "After atomicBlock");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new AtomicBlockModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");

      depot.append(Arrays.asList(1, 2, 3, 4));
    }
  }
}
