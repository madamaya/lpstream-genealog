package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.function.Function;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageUpdateTsFunctionWM<T>
    implements MapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> {

  private final TimestampAssigner<T> tsAssigner;

  public LineageUpdateTsFunctionWM(TimestampAssigner<T> tsAssigner) {
    this.tsAssigner = tsAssigner;
  }

  @Override
  public L3StreamTupleContainer<T> map(L3StreamTupleContainer<T> value) throws Exception {
    L3StreamTupleContainer<T> genealogResult = new L3StreamTupleContainer<>(value.tuple());
    GenealogMapHelper.INSTANCE.annotateResult(value, genealogResult);
    genealogResult.setLineageReliable(value.getLineageReliable());
    genealogResult.copyTimes(value);
    genealogResult.setTimestamp(tsAssigner.extractTimestamp(value.tuple(), -1));
    genealogResult.setPartitionId(value.getPartitionId());
    return genealogResult;
  }
}
