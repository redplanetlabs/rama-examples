package rama.examples.paths;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import java.util.*;

public class SortedMapRangeModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*purchaseDepot", Depot.hashBy(Ops.FIRST));

    StreamTopology s = topologies.stream("products");
    s.pstate(
      "$$totalsByTime",
      PState.mapSchema(
        String.class,
        PState.mapSchema(
          String.class,
          PState.mapSchema(Integer.class, Long.class).subindexed()
          ).subindexed()));

    s.source("*purchaseDepot").out("*tuple")
     .each(Ops.EXPAND, "*tuple").out("*storeId", "*productId", "*minuteBucket", "*amt")
     .compoundAgg(
       "$$totalsByTime",
       CompoundAgg.map(
         "*storeId",
         CompoundAgg.map(
           "*productId",
           CompoundAgg.map(
             "*minuteBucket",
             Agg.sum("*amt")))));
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new SortedMapRangeModule();
      cluster.launchModule(module, new LaunchConfig(4, 2));
      String moduleName = module.getClass().getName();

      Depot purchaseDepot = cluster.clusterDepot(moduleName, "*purchaseDepot");
      PState totalsByTime = cluster.clusterPState(moduleName, "$$totalsByTime");

      purchaseDepot.append(Arrays.asList("blockbuster", "witness-for-the-prosecution", 10, 3));
      purchaseDepot.append(Arrays.asList("blockbuster", "witness-for-the-prosecution", 10, 11));
      purchaseDepot.append(Arrays.asList("blockbuster", "witness-for-the-prosecution", 11, 12));
      purchaseDepot.append(Arrays.asList("blockbuster", "witness-for-the-prosecution", 15, 4));
      purchaseDepot.append(Arrays.asList("blockbuster", "witness-for-the-prosecution", 16, 17));
      purchaseDepot.append(Arrays.asList("blockbuster", "all-about-eve", 10, 1));
      purchaseDepot.append(Arrays.asList("blockbuster", "stangers-on-a-train", 11, 9));
      purchaseDepot.append(Arrays.asList("blockbuster", "the-best-years-of-our-lives", 10, 3));
      purchaseDepot.append(Arrays.asList("blockbuster", "inherit-the-wind", 10, 4));
      purchaseDepot.append(Arrays.asList("blockbuster", "paths-of-glory", 12, 5));

      System.out.println(
        "Query 1: " +
        totalsByTime.select(Path.key("blockbuster", "witness-for-the-prosecution")
                                .sortedMapRange(10, 20)
                                .all()
                                .filterSelected(Path.last().filterGreaterThan(10))));
      System.out.println(
        "Query 2: " +
        totalsByTime.select(Path.key("blockbuster").sortedMapRangeFrom("", 5).mapKeys()));
    }
  }
}
