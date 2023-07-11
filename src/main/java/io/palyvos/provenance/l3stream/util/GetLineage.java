package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Set;

public class GetLineage implements MapFunction<L3StreamTuple, Tuple2<L3StreamTuple, Set>> {

    private final GenealogGraphTraverser genealogGraphTraverser;

    public GetLineage(L3Settings settings) {
        this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
    }

    @Override
    public Tuple2<L3StreamTuple, Set> map(L3StreamTuple value) throws Exception {
        return Tuple2.of(value, genealogGraphTraverser.getProvenance(value));
    }
}
