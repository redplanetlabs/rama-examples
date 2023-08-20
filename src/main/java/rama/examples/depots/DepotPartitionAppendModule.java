package rama.examples.depots;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;

public class DepotPartitionAppendModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareDepot("*incomingDepot", Depot.hashBy(Ops.FIRST));
    setup.declareDepot("*outgoingDepot", Depot.disallow());

    StreamTopology s = topologies.stream("s");
    s.source("*incomingDepot").out("*tuple")
     .each(Ops.EXPAND, "*tuple").out("*k", "*k2", "*v")
     .hashPartition("*k2")
     .each(Ops.TUPLE, "*k2", new Expr(Ops.INC, "*v")).out("*newTuple")
     .depotPartitionAppend("*outgoingDepot", "*newTuple");
  }
}