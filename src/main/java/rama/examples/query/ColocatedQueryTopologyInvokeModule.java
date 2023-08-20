package rama.examples.query;

import com.rpl.rama.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

public class ColocatedQueryTopologyInvokeModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    topologies.query("q1", "*a").out("*res")
              .each(Ops.TIMES, "*a", 3).out("*res")
              .originPartition();

    topologies.query("q2", "*a").out("*res")
              .invokeQuery("q1", "*a").out("*v")
              .each(Ops.PLUS, "*v", 1).out("*res")
              .originPartition();
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new ColocatedQueryTopologyInvokeModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      QueryTopologyClient<Integer> q2 = cluster.clusterQuery(moduleName, "q2");

      System.out.println("Query 1: " + q2.invoke(1));
      System.out.println("Query 2: " + q2.invoke(10));
    }
  }
}