package io.palyvos.provenance.l3stream.wrappers.operators;

import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;

/* Add copyright (C) 2023 Masaya Yamada */

public interface L3OpWrapperStrategy {
    public RichMapFunction<KafkaInputString, L3StreamTupleContainer<KafkaInputString>> initMap(ExperimentSettings settings);

    public RichMapFunction<KafkaInputString, L3StreamTupleContainer<KafkaInputString>> initMap(ExperimentSettings settings, int sourceID);

    public <T> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> extractInputTs(WatermarkStrategy<T> watermarkStrategy);

    public <T> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> assignChkTs(WatermarkStrategy<T> watermarkStrategy, int sourceID);

    public <T> FilterFunction<L3StreamTupleContainer<T>> filter(FilterFunction<T> delegate);

    public <T, KEY> KeySelector<L3StreamTupleContainer<T>, KEY> keyBy(KeySelector<T, KEY> delegate);

    public <T, KEY> KeySelector<L3StreamTupleContainer<T>, KEY> keyBy(KeySelector<T, KEY> delegate, Class<KEY> clazz);

    public <IN, ACC, OUT> AggregateFunction<L3StreamTupleContainer<IN>, GenealogAccumulator<ACC>, L3StreamTupleContainer<OUT>> aggregate(AggregateFunction<IN, ACC, OUT> delegate);

    public <IN, ACC, OUT> AggregateFunction<L3StreamTupleContainer<IN>, GenealogAccumulator<ACC>, L3StreamTupleContainer<OUT>> aggregateTs(AggregateFunction<IN, ACC, OUT> delegate);

    public <T, O> MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> map(MapFunction<T, O> delegate);

    public <T, O> MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> mapTs(MapFunction<T, O> delegate);

    public <T, O> RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> richMap(RichMapFunction<T, O> delegate);

    public <T, O> FlatMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> flatMap(FlatMapFunction<T, O> delegate);

    public <T, O> RichFlatMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<O>> flatMap(RichFlatMapFunction<T, O> delegate);

    public <IN1, IN2, OUT> JoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> join(JoinFunction<IN1, IN2, OUT> delegate);

    public <IN1, IN2, OUT> JoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> joinTs(JoinFunction<IN1, IN2, OUT> delegate);

    public <IN1, IN2, OUT> ProcessJoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> processJoin(ProcessJoinFunction<IN1, IN2, OUT> delegate);

    public <IN1, IN2, OUT> ProcessJoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> processJoinTs(ProcessJoinFunction<IN1, IN2, OUT> delegate);

    public <T> WatermarkStrategy<L3StreamTupleContainer<T>> assignTimestampsAndWatermarks(WatermarkStrategy<T> delegate, int numOfPartitions);

    public <T> ProcessFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> extractTs();
}
