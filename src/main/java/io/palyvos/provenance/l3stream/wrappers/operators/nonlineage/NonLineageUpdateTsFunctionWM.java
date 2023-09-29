package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.function.Function;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageUpdateTsFunctionWM<T>
    implements MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> {

  public NonLineageUpdateTsFunctionWM(TimestampAssigner<T> tsAssigner) {

  }

  @Override
  public L3StreamTupleContainer<T> map(L3StreamTupleContainer<T> value) throws Exception {
    L3StreamTupleContainer<T> genealogResult = new L3StreamTupleContainer<>(value);
    genealogResult.copyTimes(value);
    return genealogResult;
  }
}
