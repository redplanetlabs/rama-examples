package rama.examples.paths;

import java.util.*;
import com.rpl.rama.*;
import com.rpl.rama.ops.Ops;
import com.rpl.rama.ops.RamaFunction1;
import com.rpl.rama.test.TestPState;

public class PathExamples {
  public static Map genData() {
    Map data = new HashMap() {{
      put("a0", new HashMap() {{
        put("a1", Arrays.asList(9, 3, 6));
        put("b1", Arrays.asList(0, 8));
      }});
      put("b0", new HashMap() {{
        put("c1", Arrays.asList("x", "y"));
      }});
    }};
    return data;
  }

  public static void simpleExample() {
    Map data = genData();

    Block.select(data, Path.key("a0").key("a1").nth(1)).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void multiNavigateExample() {
    Map data = genData();

    Block.select(data, Path.key("a0").mapVals().all()).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void filteringExample() {
    Map data = genData();
    Block.select(data, Path.key("a0").mapVals().all().filterLessThan(7)).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void firstExample() {
    Map data = genData();
    Block.select(data, Path.key("b0", "c1").first()).out("*v")
         .each(Ops.PRINTLN, "Val (A):", "*v")
         .select(data, Path.key("z").first()).out("*v")
         .each(Ops.PRINTLN, "Val (B):", "*v")
         .execute();
  }

  public static void allMapExample() {
    Map data = genData();
    Block.select(data, Path.key("a0").all()).out("*entry")
         .each(Ops.PRINTLN, "Val:", "*entry")
         .execute();
  }

  public static void filterPredExample() {
    Map data = genData();
    Block.select(data,
                 Path.mapVals()
                     .filterPred((Map m) -> m.size() == 1)
                     .mapVals()
                     .all()).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void filterSelectedExample() {
    Map data = genData();
    Block.select(data,
                 Path.mapVals()
                     .filterSelected(Path.mapVals().all().filterEqual(8))
                     .key("a1")
                     .all()).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void subListExample() {
    Map data = genData();
    Block.select(data, Path.key("a0", "a1").sublist(1, 3)).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void nullToValExample() {
    Map data = new HashMap();
    data.put("a", 1);

    Block.select(data, Path.key("a").nullToVal("xyz")).out("*v")
         .each(Ops.PRINTLN, "Val 1:", "*v")
         .select(data, Path.key("b").nullToVal("xyz")).out("*v")
         .each(Ops.PRINTLN, "Val 2:", "*v")
         .execute();
  }

  public static class MyFunction implements RamaFunction1<List, String> {
    private String token;

    public MyFunction(String token) {
      this.token = token;
    }

    @Override
    public String invoke(List l) {
      return "" + l.size() + "/" + token + "/" + l.get(0);
    }
  }

  public static void viewExamples() {
    Map data = new HashMap();
    data.put("a", Arrays.asList(1, 5, 10));

    Block.select(data, Path.key("a").nth(0).view(Ops.INC)).out("*v")
         .each(Ops.PRINTLN, "Val 1:", "*v")
         .select(data, Path.key("a").view((List l) -> l.size() + 10)).out("*v")
         .each(Ops.PRINTLN, "Val 2:", "*v")
         .select(data, Path.key("a").view(new MyFunction("***"))).out("*v")
         .each(Ops.PRINTLN, "Val 3:", "*v")
         .execute();
  }

  public static void transformedExample() {
    Map data = new HashMap();
    data.put("a", Arrays.asList(1, 5, 10));

    Block.select(data, Path.key("a").transformed(Path.nth(1).termVal("!"))).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void stayStopExamples() {
    Map data = new HashMap();
    data.put("a", 1);

    Block.select(data, Path.key("a").stay()).out("*v")
         .each(Ops.PRINTLN, "Val 1:", "*v")
         .select(data, Path.key("a").stop()).out("*v")
         .each(Ops.PRINTLN, "Val 2:", "*v")
         .execute();
  }

  public static void ifPathExamples() {
    Map data = new HashMap();
    data.put("a", Arrays.asList(1, 5, 10));

    Block.select(
            data,
            Path.key("a")
                .ifPath(Path.nth(0).filterEqual(1),
                        Path.nth(1),
                        Path.nth(2))).out("*v")
         .each(Ops.PRINTLN, "Val 1:", "*v")
         .select(
           data,
           Path.key("a")
               .ifPath(Path.all().filterEqual("x"),
                       Path.nth(1),
                       Path.nth(2))).out("*v")
         .each(Ops.PRINTLN, "Val 2:", "*v")
         .select(
           data,
           Path.key("a")
               .ifPath(Path.stop(), Path.stay())
               .nth(0)).out("*v")
         .each(Ops.PRINTLN, "Val 3:", "*v")
         .execute();
  }

  public static void multiPathExample() {
    Map data = new HashMap();
    data.put("a", 1);
    data.put("b", 2);
    data.put("c", 3);

    Block.select(data, Path.multiPath(Path.key("a"), Path.key("b"))).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void subselectExample() {
    Map data = new HashMap();
    data.put("a", Arrays.asList(1, 2, 3));
    data.put("b", Arrays.asList(4, 5));

    Block.select(data, Path.mapVals().subselect(Path.all().filterPred(Ops.IS_ODD))).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void customNavExample() {
    List data1 = new ArrayList();
    data1.add(10);
    data1.add(11);
    data1.add(12);
    List data2 = new ArrayList();
    data2.add(1);
    Block.select(data1, Path.customNav(new MyListNav())).out("*v")
         .each(Ops.PRINTLN, "Val 1:", "*v")
         .select(data2, Path.customNav(new MyListNav())).out("*v")
         .each(Ops.PRINTLN, "Val 2:", "*v")
         .execute();
  }

  public static void collectExample() {
    Map data = new HashMap();
    data.put("a", 1);
    data.put("b", 2);

    Block.select(data,
                 Path.collectOne(Path.key("b"))
                     .key("a")).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void collectExample2() {
    Map data = new HashMap();
    data.put("a", 1);
    data.put("b", 2);

    Block.select(data,
      Path.collect(Path.mapKeys())
          .multiPath(Path.key("a"), Path.key("b"))
          .putCollected("xyz")
          .collectOne(Path.stay())).out("*v")
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void customNavBuilderExamples() {
    Map data = new HashMap();
    data.put("a", 1);
    data.put("bc", 2);

    Block.select(data,
                 Path.customNavBuilder((String k) -> new CustomKey(k), "a")).out("*v")
         .each(Ops.PRINTLN, "Val 1:", "*v")
         .select(data,
           Path.customNavBuilder(
            (String arg1, String arg2) -> new CustomKey(arg1 + arg2),
            "b", "c")).out("*v")
         .each(Ops.PRINTLN, "Val 2:", "*v")
         .execute();
  }

  public static void mapKeyExample() throws Exception {
    try(TestPState tp = TestPState.create(Object.class)) {
      Block.each(Ops.IDENTITY, tp).out("$$p")
           .localTransform("$$p", Path.key("a").termVal(0))
           .localTransform("$$p", Path.key("b").termVal(1))
           .localSelect("$$p", Path.stay()).out("*initVal")
           .each(Ops.PRINTLN, "Init:", "*initVal")
           .localTransform("$$p", Path.mapKey("a").termVal("c"))
           .localSelect("$$p", Path.stay()).out("*newVal")
           .each(Ops.PRINTLN, "After transform:", "*newVal")
           .execute();
    }
  }

  public static void setVirtualValuesExample() throws Exception {
    try(TestPState tp = TestPState.create(Object.class)) {
      Block.each(Ops.IDENTITY, tp).out("$$p")
           .localTransform("$$p", Path.voidSetElem().termVal("a"))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "After first transform:", "*v")
           .localTransform("$$p", Path.voidSetElem().termVal("f"))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "After second transform:", "*v")
           .localTransform("$$p", Path.voidSetElem().termVal("c"))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "After third transform:", "*v")
           .execute();
    }
  }

  public static void listVirtualValuesExample() throws Exception {
    try(TestPState tp = TestPState.create(Object.class)) {
      Block.each(Ops.IDENTITY, tp).out("$$p")
           .localTransform("$$p", Path.afterElem().termVal("a"))
           .localTransform("$$p", Path.afterElem().termVal("b"))
           .localTransform("$$p", Path.afterElem().termVal("c"))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "After appends:", "*v")
           .localTransform("$$p", Path.beforeElem().termVal("d"))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "After prepend:", "*v")
           .localTransform("$$p", Path.beforeIndex(2).termVal("e"))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "After insert:", "*v")
           .execute();
    }
  }

  public static void subListTransformExample() throws Exception {
    try(TestPState tp = TestPState.create(Object.class)) {
      Block.each(Ops.IDENTITY, tp).out("$$p")
           .localTransform("$$p", Path.key("k").afterElem().termVal("a"))
           .localTransform("$$p", Path.key("k").afterElem().termVal("b"))
           .localTransform("$$p", Path.key("k").afterElem().termVal("c"))
           .localTransform("$$p", Path.key("k").afterElem().termVal("d"))
           .localTransform("$$p", Path.key("k").afterElem().termVal("e"))
           .localTransform("$$p", Path.key("k").afterElem().termVal("f"))
           .localSelect("$$p", Path.key("k")).out("*v")
           .each(Ops.PRINTLN, "Initial nested list:", "*v")
           .localTransform("$$p",
                           Path.key("k")
                               .sublist(1, 5)
                               .term((List l) -> {
                                 List ret = new ArrayList(l);
                                 Collections.reverse(ret);
                                 return ret;
                               }))
           .localSelect("$$p", Path.key("k")).out("*v")
           .each(Ops.PRINTLN, "After transform:", "*v")
           .execute();
    }
  }

  public static void filteredListExample() throws Exception {
    try(TestPState tp = TestPState.create(Object.class)) {
      Block.each(Ops.IDENTITY, tp).out("$$p")
           .localTransform("$$p", Path.afterElem().termVal(0))
           .localTransform("$$p", Path.afterElem().termVal(1))
           .localTransform("$$p", Path.afterElem().termVal(2))
           .localTransform("$$p", Path.afterElem().termVal(3))
           .localTransform("$$p", Path.afterElem().termVal(4))
           .localTransform("$$p", Path.afterElem().termVal(5))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "Initial list:", "*v")
           .localTransform("$$p",
                           Path.filteredList(Path.filterPred(Ops.IS_EVEN))
                               .term((List l) -> {
                                 List ret = new ArrayList(l);
                                 Collections.reverse(ret);
                                 return ret;
                               }))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "After transform:", "*v")
           .execute();
    }
  }

  public static void subselectTransformExample() throws Exception {
    try(TestPState tp = TestPState.create(Object.class)) {
      Block.each(Ops.IDENTITY, tp).out("$$p")
           .localTransform("$$p", Path.afterElem().termVal(null))
           .localTransform("$$p", Path.last().afterElem().termVal(1))
           .localTransform("$$p", Path.last().afterElem().termVal(2))
           .localTransform("$$p", Path.last().afterElem().termVal(3))
           .localTransform("$$p", Path.last().afterElem().termVal(4))
           .localTransform("$$p", Path.afterElem().termVal(null))
           .localTransform("$$p", Path.last().afterElem().termVal(5))
           .localTransform("$$p", Path.last().afterElem().termVal(6))
           .localTransform("$$p", Path.last().afterElem().termVal(7))
           .localTransform("$$p", Path.afterElem().termVal(null))
           .localTransform("$$p", Path.last().afterElem().termVal(8))
           .localTransform("$$p", Path.last().afterElem().termVal(9))
           .localTransform("$$p", Path.last().afterElem().termVal(10))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "Initial contents:", "*v")
           .localTransform("$$p",
                           Path.subselect(Path.all().all().filterPred(Ops.IS_ODD))
                               .term((List l) -> {
                                 List ret = new ArrayList(l);
                                 Collections.reverse(ret);
                                 return ret;
                               }))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "After transform:", "*v")
           .execute();
    }
  }

  public static void multiArityTermExample() throws Exception {
    try(TestPState tp = TestPState.create(Object.class)) {
      Block.each(Ops.IDENTITY, tp).out("$$p")
           .localTransform("$$p", Path.key("a").termVal(1))
           .localTransform("$$p", Path.key("b").termVal(2))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "Initial value:", "*v")
           .localTransform("$$p", Path.key("a").term(Ops.PLUS, 10))
           .localTransform("$$p",
                           Path.key("b")
                               .term((Integer v, Integer arg1, Integer arg2) -> {
                                 System.out.println("term function args: " + v + ", " + arg1 + ", " + arg2);
                                 return v * arg1 + arg2;
                               }, 10, 20))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "After transforms:", "*v")
           .execute();
    }
  }

  public static void termVoidExample() throws Exception {
    try(TestPState tp = TestPState.create(Object.class)) {
      Block.each(Ops.IDENTITY, tp).out("$$p")
           .localTransform("$$p", Path.key("a").afterElem().termVal(1))
           .localTransform("$$p", Path.key("a").afterElem().termVal(2))
           .localTransform("$$p", Path.key("a").afterElem().termVal(3))
           .localTransform("$$p", Path.key("b").termVal("xyz"))
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "Initial value:", "*v")
           .localTransform("$$p", Path.key("a").nth(1).termVoid())
           .localTransform("$$p", Path.key("b").termVoid())
           .localSelect("$$p", Path.stay()).out("*v")
           .each(Ops.PRINTLN, "After transforms:", "*v")
           .execute();
    }
  }

  public static void main(String [] args) {
    customNavBuilderExamples();
  }
}
