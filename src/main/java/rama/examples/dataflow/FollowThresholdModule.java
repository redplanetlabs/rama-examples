package rama.examples.dataflow;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;
import java.util.*;

public class FollowThresholdModule implements RamaModule {
  private SubBatch followCounts(String microbatchVar) {
    Block b = Block.explodeMicrobatch(microbatchVar).out("*follow")
                   .each(Ops.EXPAND, "*follow").out("*userId", "*followedUserId")
                   .groupBy("*userId", Block.agg(Agg.count()).out("*count"));
    return new SubBatch(b, "*userId", "*count");
  }

  public static boolean brokeThreshold(int currCount, int newCount) {
    return currCount < 2 && newCount >= 2 || currCount < 6 && newCount >= 6;
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*followsDepot", Depot.random());

    MicrobatchTopology threshold = topologies.microbatch("threshold");
    threshold.pstate("$$followCounts", PState.mapSchema(String.class, Integer.class));
    threshold.pstate("$$forceRecomputeUsers", Set.class);

    threshold.source("*followsDepot").out("*microbatch")
             .batchBlock(
               Block.subBatch(followCounts("*microbatch")).out("*userId", "*count")
                    .localSelect("$$followCounts", Path.key("*userId").nullToVal(0)).out("*currCount")
                    .each(Ops.PLUS, "*currCount", "*count").out("*newCount")
                    .each(Ops.PRINTLN, "User", "*userId", "*currCount", "->", "*newCount")
                    .ifTrue(new Expr(FollowThresholdModule::brokeThreshold, "*currCount", "*newCount"),
                      Block.each(Ops.PRINTLN, "User broke threshold:", "*userId")
                           .localTransform("$$forceRecomputeUsers", Path.voidSetElem().termVal("*userId")))
                    .localTransform("$$followCounts", Path.key("*userId").termVal("*newCount")));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new FollowThresholdModule();
      cluster.launchModule(module, new LaunchConfig(2, 2));
      String moduleName = module.getClass().getName();

      Depot followsDepot = cluster.clusterDepot(moduleName, "*followsDepot");

      System.out.println("Round 1");
      followsDepot.append(Arrays.asList("jamescagney", "bettedavis"));
      followsDepot.append(Arrays.asList("vivienleigh", "dorisday"));
      followsDepot.append(Arrays.asList("dorisday", "bettedavis"));
      cluster.waitForMicrobatchProcessedCount(moduleName, "threshold", 3);

      System.out.println("Round 2");
      followsDepot.append(Arrays.asList("jamescagney", "marlonbrando"));
      followsDepot.append(Arrays.asList("jamescagney", "jacklemmon"));
      cluster.waitForMicrobatchProcessedCount(moduleName, "threshold", 5);

      System.out.println("Round 3");
      followsDepot.append(Arrays.asList("jamescagney", "henryfonda"));
      followsDepot.append(Arrays.asList("jamescagney", "lucilleball"));
      followsDepot.append(Arrays.asList("vivienleigh", "lucilleball"));
      followsDepot.append(Arrays.asList("jamescagney", "gracekelly"));
      cluster.waitForMicrobatchProcessedCount(moduleName, "threshold", 9);
    }
  }
}
