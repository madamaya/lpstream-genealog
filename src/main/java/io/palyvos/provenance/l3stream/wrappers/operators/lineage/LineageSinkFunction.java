package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.genealog.GenealogGraphTraverser;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageSinkFunction<IN> implements
    SinkFunction<L3StreamTupleContainer<IN>> {

  private final SinkFunction<IN> delegate;
  private final GenealogGraphTraverser genealogGraphTraverser;

  public LineageSinkFunction(SinkFunction<IN> delegate, ExperimentSettings settings) {
    this.delegate = delegate;
    this.genealogGraphTraverser = new GenealogGraphTraverser(settings.aggregateStrategySupplier().get());
  }

  @Override
  public void invoke(L3StreamTupleContainer<IN> value, Context context) throws Exception {
    if (value.getLineageReliable()) {
      delegate.invoke(value.tuple(), context);

    }
  }
}
