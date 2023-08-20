package rama.examples.aggregators;

import com.rpl.rama.ops.RamaAccumulatorAgg0;

public class AccumCount implements RamaAccumulatorAgg0<Integer> {
  @Override
  public Integer accumulate(Integer currVal) {
    return currVal + 1;
  }

  @Override
  public Integer initVal() {
    return 0;
  }
}
