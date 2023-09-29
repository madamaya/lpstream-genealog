package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.*;

import java.util.*;

/* Add copyright (C) 2023 Masaya Yamada */

public class LineageWatermarkStrategy2<T>
        implements WatermarkStrategy<L3StreamTupleContainer<T>> {

    private final WatermarkStrategy<T> delegate;

    public LineageWatermarkStrategy2(WatermarkStrategy<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public WatermarkGenerator<L3StreamTupleContainer<T>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new WatermarkGenerator<L3StreamTupleContainer<T>>() {
            HashMap<Long, Long> hm = new HashMap<>(){{put(0L, 0L);}};

            @Override
            public void onEvent(L3StreamTupleContainer<T> value, long l, WatermarkOutput watermarkOutput) {
                hm.put(value.getPartitionId(), l);
                // delegate.createWatermarkGenerator(context).onEvent(value.tuple(), l, watermarkOutput);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                if (hm.size() == 4) {
                    watermarkOutput.emitWatermark(new Watermark(findMinimumWM(hm) - 1));
                }
                // delegate.createWatermarkGenerator(context).onPeriodicEmit(watermarkOutput);
            }
        };
    }

    /*
    INPUT: mは Key: instanceID, Value: 最新のtimestamp
    OUTPUT: 最小のtimestamp
    PROC: mapのvalue集合をリスト変換．valueで昇順にソートして先頭のvalueを返す．
     */
    private static long findMinimumWM(Map<Long, Long> m) {
        List<Long> vList = new ArrayList<>(m.values());
        Collections.sort(vList);
        return vList.get(0);
    }


    // CNFM: デフォルトだとTimestampAssignerはingestionTimeに基づいたタイムスタンプを付与
    /*
    @Override
    public TimestampAssigner<L3StreamTupleContainer<T>> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return WatermarkStrategy.super.createTimestampAssigner(context);
    }
    */
}
