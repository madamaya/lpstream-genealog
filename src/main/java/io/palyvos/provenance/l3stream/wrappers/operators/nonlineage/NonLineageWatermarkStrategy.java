package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

/* Add copyright (C) 2023 Masaya Yamada */

public class NonLineageWatermarkStrategy<T>
        implements WatermarkStrategy<L3StreamTupleContainer<T>> {

    private final WatermarkStrategy<T> delegate;

    public NonLineageWatermarkStrategy(WatermarkStrategy<T> delegate) {
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

    // CNFM: デフォルトだとTimestampAssignerはingestionTimeに基づいたタイムスタンプを付与
    /*
    @Override
    public TimestampAssigner<L3StreamTupleContainer<T>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return WatermarkStrategy.super.createTimestampAssigner(context);
    }
    */
}
