package rama.examples.pstates;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.*;

public class CustomKeyPartitionerModule implements RamaModule {
  public static class LastPartitionFunction implements RamaFunction2<Integer, Object, Integer> {
    @Override
    public Integer invoke(Integer numPartitions, Object key) {
      return numPartitions - 1;
    }
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", Object.class).keyPartitioner(LastPartitionFunction.class);
  }
}