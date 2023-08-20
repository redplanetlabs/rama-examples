package rama.examples.depots;

import com.rpl.rama.*;
import com.rpl.rama.ops.Ops;

public class BasicDepotExamplesModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*depot1", Depot.random());
    setup.declareDepot("*depot2", Depot.hashBy(Ops.FIRST));
    setup.declareDepot("*depot3", Depot.disallow());
  }
}
