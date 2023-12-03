package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.commons.lang3.Validate;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageProcessJoinFunction<IN1, IN2, OUT> extends
    ProcessJoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>> {

  private final ProcessJoinFunction<IN1, IN2, OUT> delegate;

  public NonLineageProcessJoinFunction(
      ProcessJoinFunction<IN1, IN2, OUT> delegate) {
    Validate.notNull(delegate, "delegate");
    this.delegate = delegate;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    delegate.open(parameters);
  }

  @Override
  public void processElement(L3StreamTupleContainer<IN1> left,
      L3StreamTupleContainer<IN2> right,
      ProcessJoinFunction<L3StreamTupleContainer<IN1>, L3StreamTupleContainer<IN2>, L3StreamTupleContainer<OUT>>.Context ctx,
      Collector<L3StreamTupleContainer<OUT>> out) throws Exception {
    long ts = System.currentTimeMillis();
    delegate.processElement(left.tuple(), right.tuple(),
        (ProcessJoinFunction<IN1, IN2, OUT>.Context) ctx, new CollectorAdapter<>(left, right, out, ts));
  }


  private static class CollectorAdapter<T1, T2, O> implements Collector<O> {

    private final L3StreamTupleContainer<T1> left;
    private final L3StreamTupleContainer<T2> right;
    private final Collector<L3StreamTupleContainer<O>> delegate;
    private final long ts;

    public CollectorAdapter(L3StreamTupleContainer<T1> left, L3StreamTupleContainer<T2> right,
        Collector<L3StreamTupleContainer<O>> delegate, long ts) {
      this.left = left;
      this.right = right;
      this.delegate = delegate;
      this.ts = ts;
    }

    @Override
    public void collect(O record) {
      L3StreamTupleContainer<O> genealogResult = new L3StreamTupleContainer<>(record);
      // GenealogJoinHelper.INSTANCE.annotateResult(left, right, genealogResult);
      // genealogResult.copyTimes(first, second);
      if (left.getTimestamp() >= right.getTimestamp()) {
        genealogResult.setTimestamp(left.getTimestamp());
      } else {
        genealogResult.setTimestamp(right.getTimestamp());
      }

      if (left.getStimulusList().get(0) >= right.getStimulusList().get(0)) {
        genealogResult.setStimulusList(left.getStimulusList());
      } else {
        genealogResult.setStimulusList(right.getStimulusList());
      }
      genealogResult.setStimulusList(ts);
      delegate.collect(genealogResult);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

}
