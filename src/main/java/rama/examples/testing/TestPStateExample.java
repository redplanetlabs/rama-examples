package rama.examples.testing;

import com.rpl.rama.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.test.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class TestPStateExample {
  public static Block fooMacro(String pstateVar) {
    return Block.localTransform(pstateVar, Path.key("a").term(Ops.INC));
  }

  @Test
  public void testPStateExampleTest() throws Exception {
    try(TestPState tp = TestPState.create(PState.mapSchema(String.class, Integer.class))) {
      tp.transform(Path.key("a").termVal(10));
      assertEquals(10, (int) tp.selectOne(Path.key("a")));

      Block.each(Ops.IDENTITY, tp).out("$$p")
           .macro(fooMacro("$$p"))
           .execute();
      assertEquals(11, (int) tp.selectOne(Path.key("a")));
    }
  }
}
