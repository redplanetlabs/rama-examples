package rama.examples.dataflow;

import com.rpl.rama.*;
import com.rpl.rama.helpers.TopologyUtils;
import com.rpl.rama.module.*;
import com.rpl.rama.RamaModule.*;
import com.rpl.rama.ops.*;

import java.util.*;

public class IntermediateExamples {
  public static void explodeExample() {
    List data = Arrays.asList(1, 2, 3, 4);
    Block.each(Ops.EXPLODE, data).out("*v")
         .each(Ops.PRINTLN, "Elem:", "*v")
         .each(Ops.PRINTLN, "X")
         .execute();
  }

  public static void tupleExample() {
    Block.each(Ops.IDENTITY, 1).out("*a")
         .each(Ops.TUPLE, "*a", 3, 2).out("*tuple")
         .each(Ops.PRINTLN, "Tuple:", "*tuple")
         .execute();
  }

  public static void expandExample() {
    List tuple = Arrays.asList(1, 2, 3);
    Block.each(Ops.EXPAND, tuple).out("*a", "*b", "*c")
         .each(Ops.PRINTLN, "Elements:", "*a", "*b", "*c")
         .execute();
  }

  public static void ifDispatchExample() {
    List data = Arrays.asList("a", 1, 3L);
    Block.each(Ops.EXPLODE, data).out("*v")
         .ifTrue(new Expr(Ops.IS_INSTANCE_OF, Integer.class, "*v"),
           Block.each(Ops.PRINTLN, "Integer case", "*v"),
           Block.ifTrue(new Expr(Ops.IS_INSTANCE_OF, String.class, "*v"),
             Block.each(Ops.PRINTLN, "String case", "*v"),
             Block.ifTrue(new Expr(Ops.IS_INSTANCE_OF, Long.class, "*v"),
               Block.each(Ops.PRINTLN, "Long case", "*v"),
               Block.each(Ops.PRINTLN, "Unexpected type"))))
         .each(Ops.PRINTLN, "After")
         .execute();
  }

  public static void subSourceExample() {
    List data = Arrays.asList("a", 1, 3L);
    Block.each(Ops.EXPLODE, data).out("*v")
         .subSource("*v",
           SubSource.create(Integer.class)
                    .each(Ops.PRINTLN, "Integer case", "*v"),
           SubSource.create(String.class)
                    .each(Ops.PRINTLN, "String case", "*v"),
           SubSource.create(Long.class)
                    .each(Ops.PRINTLN, "Long case", "*v"))
         .each(Ops.PRINTLN, "After")
         .execute();
  }

  public static void eventBreakdownExample(Topologies topologies) {
    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", PState.mapSchema(Object.class, Object.class));
    s.source("*depot").out("*k")
     .localTransform("$$p", Path.key("*k").termVal(0))
     .hashPartition("$$p2", "*k")
     .loopWithVars(LoopVars.var("*i", 0),
       Block.ifTrue(new Expr(Ops.LESS_THAN, "*i", 100),
         Block.localSelect("$$p2", Path.key("*k", "*i").sortedMapRangeFrom(0, 1000)).out("*m")
              .emitLoop("*m")
              .continueLoop(new Expr(Ops.INC, "*i"))
       )).out("*m")
     .shufflePartition()
     .each(Ops.PRINTLN, "Event")
     .hashPartition("*k")
     .localTransform("$$p", Path.key("*k").term(Ops.INC));
  }

  public static void keepTrueExample() {
    List data = Arrays.asList(1, 2, 3, 4);
    Block.each(Ops.EXPLODE, data).out("*v")
         .keepTrue(new Expr(Ops.IS_EVEN, "*v"))
         .each(Ops.PRINTLN, "Val:", "*v")
         .execute();
  }

  public static void atomicBlockExample() {
    List data = Arrays.asList(1, 3, 4, 5);
    Block.each((RamaFunction0) ArrayList::new).out("*list")
         .atomicBlock(
           Block.each(Ops.EXPLODE, data).out("*v")
                .each(Ops.INC, "*v").out("*v2")
                .each((List l, Object o) -> l.add(o), "*list", "*v2"))
         .each(Ops.PRINTLN, "List:", "*list")
         .execute();
  }

  public static void branchWithHooksAndAnchorsExample() {
    Block.each(Ops.IDENTITY, 1).out("*a")
         .anchor("root")
         .each(Ops.PRINTLN, "Result 1:", new Expr(Ops.DEC, "*a"))
         .hook("root")
         .each(Ops.PRINTLN, "Result 2:", "*a")
         .execute();
  }

  public static void branchWithExplicitBranchExample() {
    Block.each(Ops.IDENTITY, 1).out("*a")
         .anchor("root")
         .branch("root",
           Block.each(Ops.PRINTLN, "Result 1:", new Expr(Ops.DEC, "*a")))
         .each(Ops.PRINTLN, "Result 2:", "*a")
         .execute();
  }

  public static void branchingTopology1(Topologies topologies) {
    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", PState.mapSchema(Object.class, Long.class));
    s.source("*depot").out("*tuple")
     .each(Ops.EXPAND, "*tuple").out("*k1", "*k2")
     .hashPartition("*k1")
     .compoundAgg("$$p", CompoundAgg.map("*k1", Agg.sum(1)))
     .hashPartition("*k2")
     .compoundAgg("$$p", CompoundAgg.map("*k2", Agg.sum(-1)));
  }

  public static void branchingTopology2(Topologies topologies) {
    StreamTopology s = topologies.stream("s");
    s.pstate("$$p", PState.mapSchema(Object.class, Long.class));
    s.source("*depot").out("*tuple")
     .each(Ops.EXPAND, "*tuple").out("*k1", "*k2")
     .anchor("root")
     .hashPartition("*k1")
     .compoundAgg("$$p", CompoundAgg.map("*k1", Agg.sum(1)))
     .branch("root",
       Block.hashPartition("*k2")
            .compoundAgg("$$p", CompoundAgg.map("*k2", Agg.sum(-1))));
  }

  public static void batchBlockExample() {
    List data = Arrays.asList(1, 2, 3, 4);
    Block.each(Ops.PRINTLN, "Starting batch block")
         .batchBlock(
           Block.each(Ops.EXPLODE, data).out("*v")
                .each(Ops.PRINTLN, "Data:", "*v")
                .agg(Agg.count()).out("*count")
                .agg(Agg.count()).out("*sum")
                .each(Ops.PRINTLN, "Count:", "*count")
                .each(Ops.PRINTLN, "Sum:", "*sum"))
         .each(Ops.PRINTLN, "Finished batch block")
         .execute();
  }

  public static void mergeExample() {
    List source1 = Arrays.asList(1, 2);
    List source2 = Arrays.asList(2, 3);
    Block.batchBlock(
           Block.each(Ops.EXPLODE, source1).out("*v")
                .each(Ops.PRINTLN, "Source 1:", "*v")
                .anchor("source1")

                .freshBatchSource()
                .each(Ops.EXPLODE, source2).out("*v")
                .each(Ops.PRINTLN, "Source 2:", "*v")
                .anchor("source2")

                .unify("source1", "source2")
                .each(Ops.PRINTLN, "Merged:", "*v")

                .agg(Agg.sum("*v")).out("*sum")
                .each(Ops.PRINTLN, "Sum:", "*sum"))
         .execute();
  }

  public static void joinExample() {
    List source1 = Arrays.asList(Arrays.asList("a", 1),
                                 Arrays.asList("b", 2),
                                 Arrays.asList("c", 3));
    List source2 = Arrays.asList(Arrays.asList("a", 10),
                                 Arrays.asList("a", 11),
                                 Arrays.asList("c", 30));
    Block.batchBlock(
      Block.each(Ops.EXPLODE, source1).out("*tuple1")
           .each(Ops.EXPAND, "*tuple1").out("*k", "*v1")

           .freshBatchSource()
           .each(Ops.EXPLODE, source2).out("*tuple2")
           .each(Ops.EXPAND, "*tuple2").out("*k", "*v2")

           .each(Ops.PRINTLN, "Joined:", "*k", "*v1", "*v2"))
         .execute();
  }

  private static SubBatch wordCount(List source) {
    Block b = Block.each(Ops.EXPLODE, source).out("*k")
                   .compoundAgg(CompoundAgg.map("*k", Agg.count())).out("*m")
                   .each(Ops.EXPLODE_MAP, "*m").out("*k", "*count");
    return new SubBatch(b, "*k", "*count");
  }

  public static void simpleSubbatchExample() {
    List source = Arrays.asList("a", "b" ,"c", "a");
    Block.batchBlock(
      Block.subBatch(wordCount(source)).out("*k", "*c")
           .each(Ops.PRINTLN, "From subbatch:", "*k", "*c")
           .agg(Agg.max("*c")).out("*maxCount")
           .each(Ops.PRINTLN, "Max count:", "*maxCount"))
         .execute();
  }

  public static void invalidPreaggFromShadowingExample() {
    Block.batchBlock(
      Block.each(Ops.IDENTITY, 1).out("*a")
           .each(Ops.INC, "*a").out("*a")
           .agg(Agg.sum("*a")).out("*sum"))
         .execute();
  }

  public static void invalidPreaggFromStrayBranch() {
    Block.batchBlock(
      Block.each((OutputCollector collector) -> {
                  collector.emitStream("streamA", 2);
                  collector.emit(1);
                }).outStream("streamA", "streamAAnchor", "*v")
                  .out("*v")
           .agg(Agg.sum("*v")).out("*sum"))
         .execute();
  }

  public static void invalidPreaggFromNoValidJoins() {
    Block.batchBlock(
      Block.each(Ops.IDENTITY, 1).out("*a")

           .freshBatchSource()
           .each(Ops.IDENTITY, 2).out("*b"))
         .execute();
  }

  public static void invalidPreaggFromNoValidJoins2() {
    Block.batchBlock(
      Block.each(Ops.IDENTITY, 1).out("*a")

           .freshBatchSource()
           .each(Ops.IDENTITY, 2).out("*b")

           .each(Ops.PRINTLN, "Vals:", "*a", "*b"))
         .execute();
  }

  public static void outerJoinExample() {
    List source1 = Arrays.asList(Arrays.asList("a", 1),
      Arrays.asList("b", 2),
      Arrays.asList("c", 3));
    List source2 = Arrays.asList(Arrays.asList("a", 10),
      Arrays.asList("b", 20),
      Arrays.asList("d", 40));
    Block.batchBlock(
      Block.each(Ops.EXPLODE, source1).out("*tuple1")
           .each(Ops.EXPAND, "*tuple1").out("*k", "*v1")

           .freshBatchSource()
           .each(Ops.EXPLODE, source2).out("*___tuple2")
           .each(Ops.EXPAND, "*___tuple2").out("*k", "**v2")

           .each(Ops.PRINTLN, "Joined:", "*k", "*v1", "**v2"))
         .execute();
  }

  public static Block extractJavaFieldsNonMacro(Block.Impl b, Object from, String... fieldVars) {
    for(String f: fieldVars) {
      String name = f.substring(1);
      b = b.each(new TopologyUtils.ExtractJavaField(name), from).out(f);
    }
    return b;
  }

  public static Block extractJavaFieldsMacroFlawed(Object from, String... fieldVars) {
    Block.Impl ret = Block.create();
    for(String f: fieldVars) {
      String name = f.substring(1);
      ret = ret.each(new TopologyUtils.ExtractJavaField(name), from).out(f);
    }
    return ret;
  }

  public static Block extractCombinedNameMacroFlawed(Object from, String outVar) {
    return Block.macro(extractJavaFieldsMacroFlawed(from, "*firstName", "*lastName"))
                .each(Ops.TO_STRING, "*firstName", " ", "*lastName").out(outVar);
  }

  public static Block extractJavaFieldsMacro(Object from, String... fieldVars) {
    Block.Impl ret = Block.create();
    for(String f: fieldVars) {
      String name;
      if(Helpers.isGeneratedVar(f)) name = Helpers.getGeneratedVarPrefix(f);
      else name = f.substring(1);
      ret = ret.each(new TopologyUtils.ExtractJavaField(name), from).out(f);
    }
    return ret;
  }

  public static Block extractCombinedNameMacro(Object from, String outVar) {
    String firstNameVar = Helpers.genVar("firstName");
    String lastNameVar = Helpers.genVar("lastName");
    return Block.macro(extractJavaFieldsMacro(from, firstNameVar, lastNameVar))
                .each(Ops.TO_STRING, firstNameVar, " ", lastNameVar).out(outVar);
  }

  public static void main(String[] args) {
    branchWithExplicitBranchExample();
  }
}
