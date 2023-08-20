package rama.examples.integrating;

import com.rpl.rama.*;
import com.rpl.rama.integration.*;
import com.rpl.rama.module.StreamTopology;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;

import java.io.IOException;

public class SpecializedTaskGlobalModule implements RamaModule {
  public static class MyTaskGlobal implements TaskGlobalObject {
    int _v;
    public int special;

    public MyTaskGlobal(int v) {
      _v = v;
    }

    @Override
    public void prepareForTask(int taskId, TaskGlobalContext context) {
      this.special = taskId * _v;
    }

    @Override
    public void close() throws IOException {

    }
  }

  @Override
  public void define(Setup setup, Topologies topologies) {
    setup.declareObject("*tg", new MyTaskGlobal(10));
    setup.declareDepot("*depot", Depot.random());

    StreamTopology s = topologies.stream("s");
    s.source("*depot")
     .each(Ops.CURRENT_TASK_ID).out("*taskId1")
     .each((MyTaskGlobal mtg) -> mtg.special, "*tg").out("*special1")
     .shufflePartition()
     .each(Ops.CURRENT_TASK_ID).out("*taskId2")
     .each((MyTaskGlobal mtg) -> mtg.special, "*tg").out("*special2")
     .each(Ops.PRINTLN, "Results:", "*taskId1", "->", "*special1", ",", "*taskId2", "->", "*special2");
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new SpecializedTaskGlobalModule();
      cluster.launchModule(module, new LaunchConfig(8, 4));
      String moduleName = module.getClass().getName();

      Depot depot = cluster.clusterDepot(moduleName, "*depot");
      depot.append(null);
    }
  }
}
