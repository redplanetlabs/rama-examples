package rama.examples.integrating;

import com.rpl.rama.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;

import java.util.concurrent.*;

public class EachAsyncModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.source("*depot")
     .eachAsync(() -> {
       CompletableFuture ret = new CompletableFuture();
       ExecutorService es = Executors.newSingleThreadExecutor();
       es.submit(() -> {
         ret.complete("ABCDE");
         es.shutdown();
       });
       return ret;
     }).out("*v")
     .each(Ops.PRINTLN, "Result:", "*v");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new EachAsyncModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      depot.append(null);
    }
  }
}
