package rama.examples.testing;

import com.rpl.rama.ops.OutputCollector;
import com.rpl.rama.ops.RamaOperation1;
import com.rpl.rama.test.MockOutputCollector;
import com.rpl.rama.test.MockOutputCollector.CapturedEmit;
import java.util.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class MockOutputCollectorExample {
  public static class MyOperation implements RamaOperation1<Integer> {
    @Override
    public void invoke(Integer n, OutputCollector collector) {
      collector.emitStream("somestream", 1, 2);
      for(int i=0; i < n; i++) collector.emit(i);
      collector.emitStream("somestream", 3, 4);
    }
  }

  @Test
  public void mockOutputCollectorTest() {
    MockOutputCollector collector = new MockOutputCollector();
    new MyOperation().invoke(2, collector);

    List<CapturedEmit> emits = collector.getEmits();
    assertEquals(4, emits.size());

    assertEquals("somestream", emits.get(0).getStreamName());
    assertEquals(Arrays.asList(1, 2), emits.get(0).getValues());

    assertNull(emits.get(1).getStreamName());
    assertEquals(Arrays.asList(0), emits.get(1).getValues());

    assertNull(emits.get(2).getStreamName());
    assertEquals(Arrays.asList(1), emits.get(2).getValues());

    assertEquals("somestream", emits.get(3).getStreamName());
    assertEquals(Arrays.asList(3, 4), emits.get(3).getValues());

    Map expected = new HashMap();
    expected.put(null, Arrays.asList(Arrays.asList(0), Arrays.asList(1)));
    expected.put("somestream", Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4)));
    assertEquals(expected, collector.getEmitsByStream());
  }
}
