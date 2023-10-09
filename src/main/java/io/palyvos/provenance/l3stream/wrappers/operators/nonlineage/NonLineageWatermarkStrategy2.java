package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.*;

/* Add copyright (C) 2023 Masaya Yamada */

public class NonLineageWatermarkStrategy2<T>
        implements WatermarkStrategy<L3StreamTupleContainer<T>> {

    private final WatermarkStrategy<T> delegate;

    public NonLineageWatermarkStrategy2(WatermarkStrategy<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public WatermarkGenerator<L3StreamTupleContainer<T>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<L3StreamTupleContainer<T>>() {
            @Override
            public void onEvent(L3StreamTupleContainer<T> value, long l, WatermarkOutput watermarkOutput) {
                delegate.createWatermarkGenerator(context).onEvent(value.tuple(), l, watermarkOutput);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                delegate.createWatermarkGenerator(context).onPeriodicEmit(watermarkOutput);
            }
        };
    }
}