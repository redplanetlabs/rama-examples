package rama.examples.wordcount;

import com.rpl.rama.*;
import com.rpl.rama.ops.*;
import com.rpl.rama.module.*;

public class WordCountModule implements RamaModule {
    @Override
    public void define(Setup setup, Topologies topologies) {
        setup.declareDepot("*sentenceDepot", Depot.random());

        StreamTopology wordCount = topologies.stream("wordCount");
        wordCount.pstate("$$wordCounts", PState.mapSchema(String.class, Long.class));

        wordCount.source("*sentenceDepot").out("*sentence")
                 .each((String sentence, OutputCollector collector) -> {
                     for(String word: sentence.split(" ")) {
                       collector.emit(word);
                     }
                 }, "*sentence").out("*word")
                 .hashPartition("*word")
                 .compoundAgg("$$wordCounts", CompoundAgg.map("*word", Agg.count()));
    }
}
