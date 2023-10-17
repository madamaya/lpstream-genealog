package io.palyvos.provenance.l3stream.wrappers.operators;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.l3stream.util.L3Settings;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.lineage.*;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;
import java.util.function.Function;
import java.util.function.Supplier;

/* Add copyright (C) 2023 Masaya Yamada */

public class LineageModeStrategy implements L3OpWrapperStrategy {

    private final Supplier<ProvenanceAggregateStrategy> aggregateStrategy;

    public LineageModeStrategy(Supplier<ProvenanceAggregateStrategy> aggregateStrategy) {
        this.aggregateStrategy = aggregateStrategy;
    }

    @Override
    public <F extends Function<ObjectNode, Long> & Serializable> MapFunction<ObjectNode, L3StreamTupleContainer<ObjectNode>> initMapLat(F timestampFunction, F stimulusFunction, ExperimentSettings settings) {
        return new LineageInitializer(timestampFunction, stimulusFunction, settings);
    }

    @Override
    public <F extends Function<ObjectNode, Long> & Serializable> RichMapFunction<ObjectNode, L3StreamTupleContainer<ObjectNode>> initMap(F timestampFunction, F stimulusFunction, ExperimentSettings settings) {
        return new LineageInitializerTh(timestampFunction, stimulusFunction, settings, "");
    }

    @Override
    public <F extends Function<ObjectNode, Long> & Serializable> RichMapFunction<ObjectNode, L3StreamTupleContainer<ObjectNode>> initMap(F timestampFunction, F stimulusFunction, ExperimentSettings settings, String flag) {
        return new LineageInitializerTh(timestampFunction, stimulusFunction, settings, flag);
    }

    @Override
    public <T, F extends Function<L3StreamTupleContainer<T>, Long> & Serializable> MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> updateTs(F tsUpdateFunction) {
        return new LineageUpdateTsFunction<>(tsUpdateFunction);
    }

    @Override
    public <T> MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> updateTs(TimestampAssigner<T> tsAssigner) {
        return new LineageUpdateTsFunctionWM<>(tsAssigner);
    }

    @Override
    public <T> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> updateTsWM(WatermarkStrategy<T> watermarkStrategy, ExperimentSettings settings, int sourceID) {
        return new LineageUpdateTsFunctionWM2<>(watermarkStrategy);
    }

    @Override
    public <T> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> countInput(L3Settings settings) {
        return new CountInput<>(settings);
    }

    @Override
    public <T> FilterFunction<L3StreamTupleContainer<T>> filter(FilterFunction<T> delegate) {
        return new LineageFilterFunction<>(delegate);
    }

    @Override
    public <T, KEY> KeySelector<L3StreamTupleContainer<T>, KEY> keyBy(KeySelector<T, KEY> delegate) {
        return new LineageKeySelector<>(delegate);
    }

    @Override
    public <T, KEY> KeySelector<L3StreamTupleContainer<T>, KEY> keyBy(KeySelector<T, KEY> delegate, Class<KEY> clazz) {
        return new LineageKeySelectorWithTypeInfo<>(delegate, clazz);
    }

    @Override
    public <IN, ACC, OUT> AggregateFunction<L3StreamTupleContainer<IN>, GenealogAccumulator<ACC>, L3StreamTupleContainer<OUT>> aggregate(AggregateFunction<IN, ACC, OUT> delegate) {
        return new LineageAggregateFunction<>(aggregateStrategy, delegate);
    }

    @Override
    public <T, O> MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> map(MapFunction<T, O> delegate) {
        return new LineageMapFunction<>(delegate);
    }

    @Override
    public <T, O> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> richMap(RichMapFunction<T, O> delegate) {
        return new LineageRichMapFunction<>(delegate);
    }

    @Override
    public <T, O> FlatMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> flatMap(FlatMapFunction<T, O> delegate) {
        return new LineageFlatMapFunction<>(delegate);
    }

    @Override
    public <T, O> RichFlatMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> flatMap(RichFlatMapFunction<T, O> delegate) {
        return new LineageRichFlatMapFunction<>(delegate);
    }

    @Override
    public <IN1, IN2, OUT> JoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> join(JoinFunction<IN1, IN2, OUT> delegate) {
        return new LineageJoinFunction<>(delegate);
    }

    @Override
    public <IN1, IN2, OUT> ProcessJoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> processJoin(ProcessJoinFunction<IN1, IN2, OUT> delegate) {
        return new LineageProcessJoinFunction<>(delegate);
    }

    @Override
    public <T> SinkFunction<L3StreamTupleContainer<T>> sink(SinkFunction<T> delegate, ExperimentSettings settings) {
        return new LineageSinkFunction<>(delegate, settings);
    }

    @Override
    public <T> WatermarkStrategy<L3StreamTupleContainer<T>> assignTimestampsAndWatermarks(WatermarkStrategy<T> delegate, int numOfPartitions) {
        return new LineageWatermarkStrategy<>(delegate, numOfPartitions);
    }

    @Override
    public <T> AscendingTimestampExtractor<L3StreamTupleContainer<T>> assignTimestampsAndWatermarks(AscendingTimestampExtractor<T> delegate) {
        // CNFM
        throw new UnsupportedOperationException();
    }
}
