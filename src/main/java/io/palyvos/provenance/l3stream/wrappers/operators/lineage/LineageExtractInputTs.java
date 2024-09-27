package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.genealog.GenealogMapHelper;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageExtractInputTs<T>
    extends RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> {

  private transient TimestampAssigner<T> tsAssigner;
  private final WatermarkStrategy<T> watermarkStrategy;

  public LineageExtractInputTs(WatermarkStrategy<T> watermarkStrategy) {
    this.watermarkStrategy = watermarkStrategy;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    tsAssigner = watermarkStrategy.createTimestampAssigner(null);
  }

  @Override
  public void close() throws Exception {
    super.close();
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
