package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.*;

/* Add copyright (C) 2023 Masaya Yamada */

public class NonLineageWatermarkStrategy<T>
        implements WatermarkStrategy<L3StreamTupleContainer<T>> {

    private final WatermarkStrategy<T> delegate;

    public NonLineageWatermarkStrategy(WatermarkStrategy<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public TimestampAssigner<L3StreamTupleContainer<T>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new TimestampAssigner<L3StreamTupleContainer<T>>() {
            @Override
            public long extractTimestamp(L3StreamTupleContainer<T> tl3StreamTupleContainer, long l) {
                return delegate.createTimestampAssigner(context).extractTimestamp(tl3StreamTupleContainer.tuple(), l);
            }
        };
    }

    @Override
    public WatermarkGenerator<L3StreamTupleContainer<T>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<L3StreamTupleContainer<T>>() {
            WatermarkGenerator<T> wg = delegate.createWatermarkGenerator(context);

            @Override
            public void onEvent(L3StreamTupleContainer<T> value, long l, WatermarkOutput watermarkOutput) {
                // delegate.createWatermarkGenerator(context).onEvent(value.tuple(), l, watermarkOutput);
                wg.onEvent(value.tuple(), l, watermarkOutput);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                // delegate.createWatermarkGenerator(context).onPeriodicEmit(watermarkOutput);
                wg.onPeriodicEmit(watermarkOutput);
            }
        };
    }
}
