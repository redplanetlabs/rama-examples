package rama.examples.query;

import com.rpl.rama.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

public class SimpleQueryModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    topologies.query("q", "*a", "*b").out("*res")
              .each(Ops.PLUS, "*a", "*b", 1).out("*res")
              .originPartition();
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new SimpleQueryModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      QueryTopologyClient<Integer> q = cluster.clusterQuery(moduleName, "q");

      System.out.println("Query 1: " + q.invoke(1, 2));
      System.out.println("Query 2: " + q.invoke(10, 7));
    }
  }
}