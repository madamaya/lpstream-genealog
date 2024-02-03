package io.palyvos.provenance.l3stream.wrappers.operators.utils;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.util.Collector;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class NonLineageCollectorAdapter<T, O> implements Collector<O> {

    private final L3StreamTupleContainer<T> input;
    private final Collector<L3StreamTupleContainer<O>> delegate;

    public NonLineageCollectorAdapter(
            L3StreamTupleContainer<T> input, Collector<L3StreamTupleContainer<O>> delegate) {
        this.input = input;
        this.delegate = delegate;
    }

    @Override
    public void collect(O record) {
        L3StreamTupleContainer<O> genealogResult = new L3StreamTupleContainer<>(record);
        // GenealogMapHelper.INSTANCE.annotateResult(input, genealogResult);
        genealogResult.copyTimes(input);
        delegate.collect(genealogResult);
    }

    @Override
    public void close() {
        delegate.close();
    }
}