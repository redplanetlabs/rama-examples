package rama.examples.tutorial;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;

public class HelloWorldModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
        setup.declareDepot("*depot", Depot.random());
        StreamTopology s = topologies.stream("s");
        s.source("*depot").out("*data")
         .each(Ops.PRINTLN, "*data");
    }

    public static void main(String[] args) throws Exception {
        try (InProcessCluster cluster = InProcessCluster.create()) {
            cluster.launchModule(new HelloWorldModule(), new LaunchConfig(1, 1));

            String moduleName = HelloWorldModule.class.getName();
            Depot depot = cluster.clusterDepot(moduleName, "*depot");
            depot.append("Hello, world!!");
        }
    }
}
