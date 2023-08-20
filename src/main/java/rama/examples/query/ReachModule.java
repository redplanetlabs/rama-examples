package rama.examples.query;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.test.*;
import java.util.Arrays;

public class ReachModule implements RamaModule {
  private SubBatch partialReachCounts(String urlVar) {
    Block b = Block.hashPartition(urlVar)
                   .localSelect("$$urlToUsers", Path.key(urlVar).all()).out("*userId")
                   .select("$$followers", Path.key("*userId").all()).out("*reachedUserId")
                   .hashPartition("*reachedUserId")
                   .agg(Agg.set("*reachedUserId")).out("*partialReachedSet")
                   .each(Ops.SIZE, "*partialReachedSet").out("*count");
    return new SubBatch(b, "*count");
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*urlsDepot", Depot.hashBy(Ops.FIRST));
    setup.declareDepot("*followsDepot", Depot.hashBy(Ops.FIRST));

    MicrobatchTopology core = topologies.microbatch("core");
    core.pstate(
      "$$urlToUsers",
      PState.mapSchema(
        String.class,
        PState.setSchema(String.class).subindexed()));
    core.pstate(
      "$$followers",
      PState.mapSchema(
        String.class,
        PState.setSchema(String.class).subindexed()));

    core.source("*urlsDepot").out("*microbatch")
        .explodeMicrobatch("*microbatch").out("*tuple")
        .each(Ops.EXPAND, "*tuple").out("*url", "*userId")
        .compoundAgg("$$urlToUsers", CompoundAgg.map("*url", Agg.set("*userId")));

    core.source("*followsDepot").out("*microbatch")
        .explodeMicrobatch("*microbatch").out("*tuple")
        .each(Ops.EXPAND, "*tuple").out("*userId", "*followerId")
        .compoundAgg("$$followers", CompoundAgg.map("*userId", Agg.set("*followerId")));

    topologies.query("reach", "*url").out("*numUniqueUsers")
              .subBatch(partialReachCounts("*url")).out("*partialCount")
              .originPartition()
              .agg(Agg.sum("*partialCount")).out("*numUniqueUsers");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new ReachModule();
      cluster.launchModule(module, new LaunchConfig(2, 2));
      String moduleName = module.getClass().getName();

      Depot urlsDepot = cluster.clusterDepot(moduleName, "*urlsDepot");
      Depot followsDepot = cluster.clusterDepot(moduleName, "*followsDepot");

      QueryTopologyClient<Integer> reach = cluster.clusterQuery(moduleName, "reach");

      urlsDepot.append(Arrays.asList("grapefruit.com", "jamescagney"));
      urlsDepot.append(Arrays.asList("grapefruit.com", "maeclarke"));
      followsDepot.append(Arrays.asList("jamescagney", "joanleslie"));
      followsDepot.append(Arrays.asList("jamescagney", "henryfonda"));
      followsDepot.append(Arrays.asList("jamescagney", "arlenefrancis"));
      followsDepot.append(Arrays.asList("jamescagney", "jacklemmon"));
      followsDepot.append(Arrays.asList("maeclarke", "henryfonda"));
      followsDepot.append(Arrays.asList("maeclarke", "charleslaughton"));
      followsDepot.append(Arrays.asList("maeclarke", "joanleslie"));
      followsDepot.append(Arrays.asList("maeclarke", "debbiereynolds"));

      cluster.waitForMicrobatchProcessedCount(moduleName, "core", 10);

      System.out.println("grapefruit.com reach: " + reach.invoke("grapefruit.com"));
    }
  }
}