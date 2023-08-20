package rama.examples.integrating;

import com.rpl.rama.integration.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TaskThreadSharedResourceTaskGlobal implements TaskGlobalObject {
  static ConcurrentHashMap<List, Closeable> sharedResources = new ConcurrentHashMap<>();

  String _connectionString;
  List _resourceId;
  Closeable _resource;
  boolean _owner = false;

  private Closeable makeResource() {
    // stubbed out for illustration purposes
    return () -> { };
  }


  public TaskThreadSharedResourceTaskGlobal(String connectionString) {
    _connectionString = connectionString;
  }

  @Override
  public void prepareForTask(int taskId, TaskGlobalContext context) {
    int taskThreadId = Collections.min(context.getTaskGroup());
    _resourceId = Arrays.asList(context.getModuleInstanceInfo().getModuleInstanceId(),
                                taskThreadId,
                                _connectionString);
    if(sharedResources.containsKey(_resourceId)) {
      _resource = sharedResources.get(_resourceId);
    } else {
      _resource = makeResource();
      sharedResources.put(_resourceId, _resource);
      _owner = true;
    }
  }

  @Override
  public void close() throws IOException {
    if(_owner) {
      _resource.close();
      sharedResources.remove(_resourceId);
    }
  }
}
