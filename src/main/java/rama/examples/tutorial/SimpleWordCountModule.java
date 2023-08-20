package rama.examples.tutorial;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;

public class SimpleWordCountModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
        setup.declareDepot("*depot", Depot.random());

        StreamTopology s = topologies.stream("s");
        s.pstate("$$wordCounts", PState.mapSchema(String.class, Long.class));

        s.source("*depot").out("*token")
         .hashPartition("*token")
         .compoundAgg("$$wordCounts", CompoundAgg.map("*token", Agg.count()));
    }

    public static void main(String[] args) throws Exception {
        try (InProcessCluster cluster = InProcessCluster.create()) {
            cluster.launchModule(new SimpleWordCountModule(), new LaunchConfig(1, 1));
            String moduleName = SimpleWordCountModule.class.getName();
            Depot depot = cluster.clusterDepot(moduleName, "*depot");
            depot.append("one");
            depot.append("two");
            depot.append("two");
            depot.append("three");
            depot.append("three");
            depot.append("three");

            PState wc = cluster.clusterPState(moduleName, "$$wordCounts");
            System.out.println("one: " + wc.selectOne(Path.key("one")));
            System.out.println("two: " + wc.selectOne(Path.key("two")));
            System.out.println("three: " + wc.selectOne(Path.key("three")));
        }
    }
}
