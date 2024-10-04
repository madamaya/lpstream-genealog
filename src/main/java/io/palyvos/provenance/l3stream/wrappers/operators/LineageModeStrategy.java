package io.palyvos.provenance.l3stream.wrappers.operators;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.l3stream.wrappers.operators.lineage.*;
import io.palyvos.provenance.l3stream.wrappers.operators.nonlineage.NonLineageWatermarkStrategy;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;

import java.util.function.Supplier;

/* Add copyright (C) 2023 Masaya Yamada */

public class LineageModeStrategy implements L3OpWrapperStrategy {

    private final Supplier<ProvenanceAggregateStrategy> aggregateStrategy;

    public LineageModeStrategy(Supplier<ProvenanceAggregateStrategy> aggregateStrategy) {
        this.aggregateStrategy = aggregateStrategy;
    }

    @Override
    public RichMapFunction<KafkaInputString, L3StreamTupleContainer<KafkaInputString>> initMap(ExperimentSettings settings) {
        return new LineageInitializerThV2(settings, 0);
    }

    @Override
    public RichMapFunction<KafkaInputString, L3StreamTupleContainer<KafkaInputString>> initMap(ExperimentSettings settings, int sourceID) {
        return new LineageInitializerThV2(settings, sourceID);
    }

    @Override
    public <T> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> extractInputTs(WatermarkStrategy<T> watermarkStrategy) {
        return new LineageExtractInputTs<>(watermarkStrategy);
    }

    @Override
    public <T> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> assignChkTs(WatermarkStrategy<T> watermarkStrategy, int sourceID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> FilterFunction<L3StreamTupleContainer<T>> filter(FilterFunction<T> delegate) {
        return new LineageFilterFunction<>(delegate);
    }

    @Override
    public <T> RichFilterFunction<L3StreamTupleContainer<T>> filter(RichFilterFunction<T> delegate) {
        return new LineageRichFilterFunction<>(delegate);
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
    public <IN, ACC, OUT> AggregateFunction<L3StreamTupleContainer<IN>, GenealogAccumulator<ACC>, L3StreamTupleContainer<OUT>> aggregateTs(AggregateFunction<IN, ACC, OUT> delegate) {
        return new LineageAggregateFunctionTs<>(aggregateStrategy, delegate);
    }

    @Override
    public <T, O> MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> map(MapFunction<T, O> delegate) {
        return new LineageMapFunction<>(delegate);
    }

    @Override
    public <T, O> MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> mapTs(MapFunction<T, O> delegate) {
        return new LineageMapFunctionTs<>(delegate);
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
    public <IN1, IN2, OUT> JoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> joinTs(JoinFunction<IN1, IN2, OUT> delegate) {
        return new LineageJoinFunctionTs<>(delegate);
    }

    @Override
    public <IN1, IN2, OUT> ProcessJoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> processJoin(ProcessJoinFunction<IN1, IN2, OUT> delegate) {
        return new LineageProcessJoinFunction<>(delegate);
    }

    @Override
    public <IN1, IN2, OUT> ProcessJoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> processJoinTs(ProcessJoinFunction<IN1, IN2, OUT> delegate) {
        return new LineageProcessJoinFunctionTs<>(delegate);
    }

    @Override
    public <T> WatermarkStrategy<L3StreamTupleContainer<T>> assignTimestampsAndWatermarks(WatermarkStrategy<T> delegate, int numOfPartitions) {
        if (numOfPartitions == 1) {
            return new NonLineageWatermarkStrategy<>(delegate);
        } else {
            return new LineageWatermarkStrategy<>(delegate, numOfPartitions);
        }
    }

    @Override
    public <T> ProcessFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> extractTs() {
        return new LineageExtractTs<>();
    }
}
