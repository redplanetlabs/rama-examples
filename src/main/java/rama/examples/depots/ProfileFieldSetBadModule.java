package rama.examples.depots;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;

public class ProfileFieldSetBadModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*profileFieldsDepot", Depot.random());

    StreamTopology profiles = topologies.stream("profiles");
    profiles.pstate(
      "$$profiles",
      PState.mapSchema(
        String.class,
        PState.mapSchema(
          String.class,
          Object.class)));

    profiles.source("*profileFieldsDepot").out("*tuple")
            .each(Ops.EXPAND, "*tuple").out("*userId", "*field", "*value")
            .hashPartition("*userId")
            .localTransform("$$profiles", Path.key("*userId", "*field").termVal("*value"));
  }
}