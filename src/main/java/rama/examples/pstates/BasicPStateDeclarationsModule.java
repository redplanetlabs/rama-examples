package rama.examples.pstates;

import com.rpl.rama.*;
import com.rpl.rama.module.*;

public class BasicPStateDeclarationsModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    StreamTopology s = topologies.stream("s");
    s.pstate("$$p1", Long.class);
    s.pstate("$$p2", PState.mapSchema(String.class, Integer.class));
    s.pstate(
      "$$p3",
      PState.mapSchema(
        String.class,
        PState.fixedKeysSchema(
          "count", Integer.class,
          "someList", PState.listSchema(String.class),
          "someSet", PState.setSchema(Integer.class))));
  }
}
