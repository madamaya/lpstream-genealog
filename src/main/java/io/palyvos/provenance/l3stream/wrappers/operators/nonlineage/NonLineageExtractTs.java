package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class NonLineageExtractTs<T> extends ProcessFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> {
    @Override
    public void processElement(L3StreamTupleContainer<T> input, ProcessFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>>.Context context, Collector<L3StreamTupleContainer<T>> collector) throws Exception {
        L3StreamTupleContainer<T> out = new L3StreamTupleContainer<>(input.tuple());
        out.copyTimesWithoutTs(input);
        out.setTimestamp(context.timestamp());
        collector.collect(out);
    }
}
