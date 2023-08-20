package rama.examples.partitioners;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;
import java.util.*;

public class CustomPartitionerModule implements RamaModule {
  public static class TaskOnePartitioner implements RamaFunction1<Integer, Integer> {
    @Override
    public Integer invoke(Integer numPartitions) {
      return 1;
    }
  }

  public static class MyPartitioner implements RamaFunction3<Integer, Integer, Integer, Integer> {
    @Override
    public Integer invoke(Integer numPartitions, Integer n1, Integer n2) {
      if(n2 > n1) return 0;
      else return numPartitions - 1;
    }
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.source("*depot").out("*tuple")
     .each(Ops.PRINTLN, "Start task", "*tuple", new Expr(Ops.CURRENT_TASK_ID))
     .customPartition(new TaskOnePartitioner())
     .each(Ops.PRINTLN, "Next task", "*tuple", new Expr(Ops.CURRENT_TASK_ID))
     .each(Ops.EXPAND, "*tuple").out("*n1", "*n2")
     .customPartition(new MyPartitioner(), "*n1", "*n2")
     .each(Ops.PRINTLN, "Final task", "*tuple", new Expr(Ops.CURRENT_TASK_ID));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new CustomPartitionerModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");

      depot.append(Arrays.asList(0, 1));
      depot.append(Arrays.asList(5, 2));
    }
  }
}
