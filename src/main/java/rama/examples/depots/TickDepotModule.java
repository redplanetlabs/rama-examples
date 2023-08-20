package rama.examples.depots;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;

public class TickDepotModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareTickDepot("*ticks", 3000);

    StreamTopology s = topologies.stream("s");
    s.source("*ticks")
     .each(Ops.PRINTLN, "Tick");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      cluster.launchModule(new TickDepotModule(), new LaunchConfig(4, 4));
      Thread.sleep(10000);
    }
  }
}
