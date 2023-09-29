package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.function.Function;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageInitializerTh extends RichMapFunction<ObjectNode, L3StreamTupleContainer<ObjectNode>> {

  private final Function<ObjectNode, Long> timestampFunction;
  private final Function<ObjectNode, Long> stimulusFunction;
  long start;
  long count;
  String flag;
  ExperimentSettings settings;

  public <F extends Function<ObjectNode, Long> & Serializable> LineageInitializerTh(F timestampFunction,
                                                                                    F stimulusFunction,
                                                                                    ExperimentSettings settings,
                                                                                    String flag) {
    this.timestampFunction = timestampFunction;
    this.stimulusFunction = stimulusFunction;
    this.settings = settings;
    this.flag = flag;
  }

  @Override
  public L3StreamTupleContainer<ObjectNode> map(ObjectNode value) throws Exception {
    L3StreamTupleContainer<ObjectNode> out = new L3StreamTupleContainer<>(value);
    out.initGenealog(GenealogTupleType.SOURCE);
    out.setLineageReliable(true);
    //out.setTimestamp(System.currentTimeMillis());
    out.setTimestamp(timestampFunction.apply(value));
    //out.setStimulus(System.nanoTime());
    out.setStimulus(stimulusFunction.apply(value));
    out.setPartitionId(value.get("metadata").get("partition").asInt());
    count++;
    return out;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    start = System.currentTimeMillis();
    count = 0L;
  }

  @Override
  public void close() throws Exception {
    long end = System.currentTimeMillis();
    PrintWriter pw = new PrintWriter(System.getenv("HOME") + "/logs/" + flag + "lineage/throughput" + getRuntimeContext().getIndexOfThisSubtask() + "_" + end + "_-short_-1_0_1000.log.log");
    pw.println(start + "," + end + "," + (end - start) + "," + count);
    pw.flush();
    pw.close();
    super.close();
  }
}
