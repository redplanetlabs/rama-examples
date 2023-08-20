package rama.examples.depots;

import com.rpl.rama.*;

public class CustomPartitioningModule implements RamaModule {
  public static class MyPartitioner implements Depot.Partitioning<Integer> {
    @Override
    public int choosePartitionIndex(Integer data, int numPartitions) {
      if(data==11) return numPartitions - 1;
      else return 0;
    }
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot", MyPartitioner.class);
  }
}
