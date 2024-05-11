package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.*;

import java.util.*;

/* Add copyright (C) 2023 Masaya Yamada */

public class LineageWatermarkStrategy<T>
        implements WatermarkStrategy<L3StreamTupleContainer<T>> {

    private final WatermarkStrategy<T> delegate;
    private final int numOfPartitions;

    public LineageWatermarkStrategy(WatermarkStrategy<T> delegate, int numOfPartitions) {
        this.delegate = delegate;
        if (numOfPartitions <= 1) {
            throw new IllegalArgumentException("LineageWatermarkStrategy: numOfPartitions = " + numOfPartitions + " <= 1");
        }
        this.numOfPartitions = numOfPartitions;
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
            HashMap<Long, Long> hm = new HashMap<>();

            @Override
            public void onEvent(L3StreamTupleContainer<T> value, long l, WatermarkOutput watermarkOutput) {
                hm.put(value.getPartitionId(), l);
            }

            @Override
            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
                if (hm.size() == numOfPartitions) {
                    watermarkOutput.emitWatermark(new Watermark(findMinimumWM(hm) - 1));
                }
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
}
