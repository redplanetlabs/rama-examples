package rama.examples.pstates;

import com.rpl.rama.*;
import com.rpl.rama.diffs.Diff;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;
import java.util.*;

public class MultiSubscriberModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", Depot.hashBy(Ops.IDENTITY));

    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", Map.class).global();
    s.source("*depot").out("*k")
     .localTransform("$$p", Path.key("*k").nullToVal(0).term(Ops.INC));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new MultiSubscriberModule();
      cluster.launchModule(module, new LaunchConfig(1, 1));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      PState p = cluster.clusterPState(moduleName, "$$p");

      depot.append("a");
      depot.append("b");
      depot.append("c");

      ProxyState<Map> proxyTop = p.proxy(Path.stay(), new ProxyState.Callback<Map>() {
        @Override
        public void change(Map newVal, Diff diff, Map oldVal) {
          System.out.println("'top' callback: " + newVal + ", " + diff + ", " + oldVal);
        }
      });

      ProxyState<Integer> proxyA = p.proxy(Path.key("a"), new ProxyState.Callback<Integer>() {
        @Override
        public void change(Integer newVal, Diff diff, Integer oldVal) {
          System.out.println("'a' callback: " + newVal + ", " + diff + ", " + oldVal);
        }
      });

      ProxyState<Integer> proxyB = p.proxy(Path.key("b"), new ProxyState.Callback<Integer>() {
        @Override
        public void change(Integer newVal, Diff diff, Integer oldVal) {
          System.out.println("'b' callback: " + newVal + ", " + diff + ", " + oldVal);
        }
      });

      depot.append("a");
      depot.append("a");
      depot.append("c");
      depot.append("b");

      Thread.sleep(50);
    }
  }
}
