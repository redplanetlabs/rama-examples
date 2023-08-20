package rama.examples.wordcount;

import com.rpl.rama.*;
import com.rpl.rama.module.*;
import com.rpl.rama.test.*;

public class WordCountRunner {
    public static void main(String[] args) throws Exception {
        try (InProcessCluster cluster = InProcessCluster.create()) {
            cluster.launchModule(new WordCountModule(), new LaunchConfig(4, 2));
            String moduleName = WordCountModule.class.getName();
            Depot depot = cluster.clusterDepot(moduleName, "*sentenceDepot");
            depot.append("hello world");
            depot.append("hello world again");
            depot.append("say hello to the planet");
            depot.append("red planet labs");

            PState wc = cluster.clusterPState(moduleName, "$$wordCounts");
            System.out.println("'hello' count: " + wc.selectOne(Path.key("hello")));
            System.out.println("'world' count: " + wc.selectOne(Path.key("world")));
            System.out.println("'planet' count: " + wc.selectOne(Path.key("planet")));
            System.out.println("'red' count: " + wc.selectOne(Path.key("red")));
        }
    }
}
