package rama.examples.dataflow;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.ops.Ops;

public class NonMacroModuleUniqueIdPState {
  private final String _pstateName;

  public NonMacroModuleUniqueIdPState(String pstateName) {
    _pstateName = pstateName;
  }

  public void declarePState(ETLTopologyBase topology) {
    topology.pstate(_pstateName, Long.class).initialValue(0L);
  }

  private static long generateId(Long id1, Integer taskId) {
    return (((long) taskId) << 42) | id1;
  }

  public Block.Impl genId(Block.Impl b, String outVar) {
    String counterVar = Helpers.genVar("counter");
    String taskIdVar = Helpers.genVar("taskId");
    b = b.localSelect(_pstateName, Path.stay()).out(counterVar)
         .localTransform(_pstateName, Path.term(Ops.PLUS_LONG, 1))
         .each(Ops.CURRENT_TASK_ID).out(taskIdVar)
         .each(NonMacroModuleUniqueIdPState::generateId, counterVar, taskIdVar).out(outVar);
    return b;
  }
}
