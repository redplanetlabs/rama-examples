package rama.examples.depots;

import com.rpl.rama.Depot;
import com.rpl.rama.RamaModule;

public class GlobalDepotModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*myGlobalDepot", Depot.random()).global();
  }
}
