package rama.examples.mirrors;

import com.rpl.rama.RamaModule;

public class MirrorDeclarationsModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.clusterPState("$$mirror", "com.mycompany.FooModule", "$$p");
    setup.clusterDepot("*mirrorDepot", "com.mycompany.FooModule", "*depot");
    setup.clusterQuery("*mirrorQuery", "com.mycompany.BarModule", "someQueryTopologyName");
  }
}
