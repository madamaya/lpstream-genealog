package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;

/* Add copyright (C) 2023 Masaya Yamada */

public class NonLineageAscendingTimestampExtractor<T>
        extends AscendingTimestampExtractor<L3StreamTupleContainer<T>> {

    private final AscendingTimestampExtractor<T> delegate;

    public NonLineageAscendingTimestampExtractor(AscendingTimestampExtractor<T> delegate) {
        this.delegate = delegate;
    }


    @Override
    public long extractAscendingTimestamp(L3StreamTupleContainer<T> value) {
        return delegate.extractAscendingTimestamp(value.tuple());
    }
}
