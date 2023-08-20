package rama.examples.aggregators;

import com.rpl.rama.ops.RamaCombinerAgg;

public class CombinerSum implements RamaCombinerAgg<Integer> {
  @Override
  public Integer combine(Integer curr, Integer arg) {
    return curr + arg;
  }

  @Override
  public Integer zeroVal() {
    return 0;
  }
}
