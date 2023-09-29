package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.utils.LineageCollectorAdapter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class LineageRichFlatMapFunction<T, O>
        extends RichFlatMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> {
    private final RichFlatMapFunction<T, O> delegate;

    public LineageRichFlatMapFunction(RichFlatMapFunction<T, O> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.delegate.open(parameters);
    }

    @Override
    public void flatMap(L3StreamTupleContainer<T> value, Collector<L3StreamTupleContainer<O>> out) throws Exception {
        delegate.flatMap(value.tuple(), new LineageCollectorAdapter<>(value, out));
    }

    @Override
    public void close() throws Exception {
        this.delegate.close();
    }
}
