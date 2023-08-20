package rama.examples.query;

import com.rpl.rama.*;
import com.rpl.rama.test.*;
import com.rpl.rama.ops.*;

public class FibonacciModule implements RamaModule {
  @Override
  public void define(Setup setup, Topologies topologies) {
    topologies.query("fib", "*n").out("*res")
              .ifTrue(new Expr(Ops.OR, new Expr(Ops.EQUAL, "*n", 0),
                                       new Expr(Ops.EQUAL, "*n", 1)),
                Block.each(Ops.IDENTITY, 1).out("*res"),
                Block.invokeQuery("fib", new Expr(Ops.DEC, "*n")).out("*a")
                     .invokeQuery("fib", new Expr(Ops.MINUS, "*n", 2)).out("*b")
                     .each(Ops.PLUS, "*a", "*b").out("*res"))
              .originPartition();
  }

  public static void main(String[] args) throws Exception {
    try(InProcessCluster cluster = InProcessCluster.create()) {
      RamaModule module = new FibonacciModule();
      cluster.launchModule(module, new LaunchConfig(4, 4));
      String moduleName = module.getClass().getName();

      QueryTopologyClient<Integer> fib = cluster.clusterQuery(moduleName, "fib");

      for(int i=0; i<10; i++) {
        System.out.println("Fib(" + i + "): " + fib.invoke(i));
      }
    }
  }
}