package rama.examples.paths;

import java.util.Map;
import com.rpl.rama.Navigator;

public class CustomKey implements Navigator<Map> {
  private final Object _k;

  public CustomKey(Object k) {
    _k = k;
  }

  @Override
  public Object select(Map obj, Next next) {
    return next.invokeNext(obj.get(_k));
  }

  @Override
  public Map transform(Map obj, Next next) {
    obj.put(_k, next.invokeNext(obj.get(_k)));
    return obj;
  }
}
