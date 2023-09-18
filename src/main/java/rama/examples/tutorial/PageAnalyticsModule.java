package rama.examples.tutorial;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import java.util.*;

/*
 * This module is from the tutorial before distributed programming and partitioners
 * are introduced: https://redplanetlabs.com/docs/~/tutorial2.html
 *
 * In reality this module would need a call to .hashPartition if deploying with
 * more than one task.
 */
public class PageAnalyticsModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
        setup.declareDepot("*depot", Depot.random());

        StreamTopology s = topologies.stream("s");
        s.pstate("$$pageViewCount", PState.mapSchema(String.class, Long.class));
        s.pstate("$$sessionHistory",
                 PState.mapSchema(
                   String.class,
                   PState.listSchema(PState.mapSchema(String.class, Object.class))));

        s.source("*depot").out("*pageVisit")
                .each((Map<String, Object> visit) -> visit.get("sessionId"), "*pageVisit").out("*sessionId")
                .each((Map<String, Object> visit) -> visit.get("path"), "*pageVisit").out("*path")
                .each((Map<String, Object> visit) -> {
                  Map ret = new HashMap(visit);
                  ret.remove("sessionId");
                  return ret;
                }, "*pageVisit").out("*thinPageVisit")
                .compoundAgg("$$pageViewCount", CompoundAgg.map("*path", Agg.count()))
                .compoundAgg("$$sessionHistory", CompoundAgg.map("*sessionId", Agg.list("*thinPageVisit")))
        ;
    }

    public static void main(String[] args) throws Exception {
        try (InProcessCluster cluster = InProcessCluster.create()) {
            cluster.launchModule(new PageAnalyticsModule(), new LaunchConfig(1, 1));
            String moduleName = PageAnalyticsModule.class.getName();
            Depot depot = cluster.clusterDepot(moduleName, "*depot");

            Map<String, Object> pageVisit = new HashMap<String, Object>();
            pageVisit.put("sessionId", "abc123");
            pageVisit.put("path", "/posts");
            pageVisit.put("duration", 4200);

            depot.append(pageVisit);
        }
    }
}
