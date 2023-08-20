package rama.examples.integrating;

import com.rpl.rama.integration.*;
import java.io.IOException;

public class TickedTaskGlobalExample implements TaskGlobalObjectWithTick {
  Integer _taskId;
  int _tick = 0;

  @Override
  public void prepareForTask(int taskId, TaskGlobalContext context) {
    _taskId = taskId;
  }

  @Override
  public long getFrequencyMillis() {
    return 30000;
  }

  @Override
  public void tick() {
    _tick++;
    System.out.println("Tick " + _tick + " on " + _taskId);
  }

  @Override
  public void close() throws IOException {

  }
}
