package rama.examples.aggregators;

import com.rpl.rama.ops.RamaAccumulatorAgg2;

public class AccumCustom implements RamaAccumulatorAgg2<String, Integer, String> {
  private String _divider;

  public AccumCustom(String divider) {
    _divider = divider;
  }

  @Override
  public String accumulate(String currVal, Integer arg0, String arg1) {
    return currVal + _divider + arg0 + _divider + arg1;
  }

  @Override
  public String initVal() {
    return "";
  }
}
