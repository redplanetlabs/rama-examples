package rama.examples.query;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;

public class TemporaryQueryStateModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    topologies.query("foo", "*v", "*v2").out("*res")
              .hashPartition("*v")
              .localTransform("$$foo$$", Path.termVal("*v2"))
              .shufflePartition()
              .hashPartition("*v")
              .localSelect("$$foo$$", Path.stay()).out("*v3")
              .originPartition()
              .each(Ops.PLUS, "*v", "*v3").out("*res");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new TemporaryQueryStateModule();
      cluster.launchModule(module, new LaunchConfig(4, 2));
      String moduleName = module.getClass().getName();

      QueryTopologyClient<Integer> foo = cluster.clusterQuery(moduleName, "foo");

      System.out.println("Result: " + foo.invoke(2, 3));
      System.out.println("Result: " + foo.invoke(2, 4));
      System.out.println("Result: " + foo.invoke(3, 5));
    }
  }
}
