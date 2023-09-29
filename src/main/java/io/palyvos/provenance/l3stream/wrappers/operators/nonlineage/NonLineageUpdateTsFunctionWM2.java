package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageUpdateTsFunctionWM2<T>
    extends RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> {

  public NonLineageUpdateTsFunctionWM2(WatermarkStrategy<T> watermarkStrategy) {

  }

  @Override
  public L3StreamTupleContainer<T> map(L3StreamTupleContainer<T> value) throws Exception {
    L3StreamTupleContainer<T> genealogResult = new L3StreamTupleContainer<>(value);
    genealogResult.copyTimes(value);
    return genealogResult;
  }
}
