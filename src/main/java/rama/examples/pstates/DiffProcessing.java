package rama.examples.pstates;

import com.rpl.rama.diffs.*;
import java.util.*;

public class DiffProcessing {
  public static class MyProcessor implements Diff.Processor, KeyDiff.Processor {
    public List processedKeys = new ArrayList();

    @Override
    public void processKeyDiff(KeyDiff diff) {
      processedKeys.add(diff.getKey());
    }

    @Override
    public void unhandled() {
      processedKeys = null;
    }
  }

  public static void processKeysDiff() {
    Map m = new HashMap(2);
    m.put("a", new NewValueDiff(1));
    m.put("x", new NewValueDiff(2));
    m.put("d", new NewValueDiff(3));
    m.put("y", new NewValueDiff(4));
    Diff diff = new KeysDiff(m);

    MyProcessor processor = new MyProcessor();
    diff.process(processor);
    System.out.println("Processed keys 1: " + processor.processedKeys);

    MyProcessor processor2 = new MyProcessor();
    new UnknownDiff().process(processor2);
    System.out.println("Processed keys 2: " + processor2.processedKeys);
  }

  public static void main(String[] args) {
    processKeysDiff();
  }
}
