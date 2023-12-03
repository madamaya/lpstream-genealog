package io.palyvos.provenance.l3stream.wrappers.operators;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInput;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.nonlineage.*;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;

import java.util.function.Supplier;

/* Add copyright (C) 2023 Masaya Yamada */

public class NonLineageModeStrategy implements L3OpWrapperStrategy {

    private final Supplier<ProvenanceAggregateStrategy> aggregateStrategy;

    public NonLineageModeStrategy(Supplier<ProvenanceAggregateStrategy> aggregateStrategy) {
        this.aggregateStrategy = aggregateStrategy;
    }

    /*
    @Override
    public <F extends Function<ObjectNode, Long> & Serializable> MapFunction<ObjectNode, L3StreamTupleContainer<ObjectNode>> initMapLat(F timestampFunction, F stimulusFunction, ExperimentSettings settings) {
        return new NonLineageInitializer(timestampFunction, stimulusFunction, settings);
    }
     */

    /*
    @Override
    public <F extends Function<ObjectNode, Long> & Serializable> RichMapFunction<ObjectNode, L3StreamTupleContainer<ObjectNode>> initMap(F timestampFunction, F stimulusFunction, ExperimentSettings settings) {
        return new NonLineageInitializerTh(timestampFunction, stimulusFunction, settings, "");
    }
     */

    @Override
    public RichMapFunction<KafkaInputString, L3StreamTupleContainer<KafkaInputString>> initMap(ExperimentSettings settings) {
        return new NonLineageInitializerThV2(settings, 0);
    }

    @Override
    public RichMapFunction<KafkaInputString, L3StreamTupleContainer<KafkaInputString>> initMap(ExperimentSettings settings, int sourceID) {
        return new NonLineageInitializerThV2(settings, sourceID);
    }

    /*
    @Override
    public <T, F extends Function<L3StreamTupleContainer<T>, Long> & Serializable> MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> updateTs(F tsUpdateFunction) {
        return new NonLineageUpdateTsFunction<>(tsUpdateFunction);
    }
     */

    /*
    @Override
    public <T> MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> updateTs(TimestampAssigner<T> tsAssigner) {
        return new NonLineageUpdateTsFunctionWM<>(tsAssigner);
    }
     */

    @Override
    public <T> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> updateTsWM(WatermarkStrategy<T> watermarkStrategy, int sourceID) {
        return new NonLineageUpdateTsFunctionWM2<>(watermarkStrategy, sourceID);
    }

    /*
    @Override
    public <T> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> countInput(L3Settings settings) {
        return new CountInput<>(settings);
    }
     */

    @Override
    public <T> FilterFunction<L3StreamTupleContainer<T>> filter(FilterFunction<T> delegate) {
        return new NonLineageFilterFunction<>(delegate);
    }

    @Override
    public <T, KEY> KeySelector<L3StreamTupleContainer<T>, KEY> keyBy(KeySelector<T, KEY> delegate) {
        return new NonLineageKeySelector<>(delegate);
    }

    @Override
    public <T, KEY> KeySelector<L3StreamTupleContainer<T>, KEY> keyBy(KeySelector<T, KEY> delegate, Class<KEY> clazz) {
        return new NonLineageKeySelectorWithTypeInfo<>(delegate, clazz);
    }

    @Override
    public <IN, ACC, OUT> AggregateFunction<L3StreamTupleContainer<IN>, GenealogAccumulator<ACC>, L3StreamTupleContainer<OUT>> aggregate(AggregateFunction<IN, ACC, OUT> delegate) {
        return new NonLineageAggregateFunction<IN, ACC, OUT>(aggregateStrategy, delegate);
    }

    @Override
    public <T, O> MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> map(MapFunction<T, O> delegate) {
        return new NonLineageMapFunction<>(delegate);
    }

    @Override
    public <T, O> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> richMap(RichMapFunction<T, O> delegate) {
        return new NonLineageRichMapFunction<>(delegate);
    }

    @Override
    public <T, O> FlatMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> flatMap(FlatMapFunction<T, O> delegate) {
        // return new NonLineageFlatMapFunction<>(delegate);
        throw new UnsupportedOperationException();
    }

    @Override
    public <T, O> RichFlatMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> flatMap(RichFlatMapFunction<T, O> delegate) {
        // return new NonLineageRichFlatMapFunction<>(delegate);
        throw new UnsupportedOperationException();
    }

    @Override
    public <IN1, IN2, OUT> JoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> join(JoinFunction<IN1, IN2, OUT> delegate) {
        return new NonLineageJoinFunction<>(delegate);
    }

    @Override
    public <IN1, IN2, OUT> ProcessJoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> processJoin(ProcessJoinFunction<IN1, IN2, OUT> delegate) {
        return new NonLineageProcessJoinFunction<>(delegate);
    }

    /*
    @Override
    public <T> SinkFunction<L3StreamTupleContainer<T>> sink(SinkFunction<T> delegate, ExperimentSettings settings) {
        return new NonLineageSinkFunction<>(delegate);
    }
     */

    @Override
    public <T> WatermarkStrategy<L3StreamTupleContainer<T>> assignTimestampsAndWatermarks(WatermarkStrategy<T> delegate, int numOfPartitions) {
        return new NonLineageWatermarkStrategy<>(delegate);
    }

    /*
    @Override
    public <T> AscendingTimestampExtractor<L3StreamTupleContainer<T>> assignTimestampsAndWatermarks(AscendingTimestampExtractor<T> delegate) {
        throw new UnsupportedOperationException();
        // return new NonLineageAscendingTimestampExtractor<>(delegate);
    }
     */


}
