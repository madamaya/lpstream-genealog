package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintWriter;
import java.io.Serializable;
import java.util.function.Function;

/* Modifications copyright (C) 2023 Masaya Yamada */

// CNFM: Latency測定用には，stimulusFunctionをセットする別のクラス実装が必要．
public class NonLineageInitializer implements MapFunction<ObjectNode, L3StreamTupleContainer<ObjectNode>> {

  private final Function<ObjectNode, Long> timestampFunction;
  private final Function<ObjectNode, Long> stimulusFunction;
  ExperimentSettings settings;

  public <F extends Function<ObjectNode, Long> & Serializable> NonLineageInitializer(F timestampFunction,
                                                                                     F stimulusFunction,
                                                                                     ExperimentSettings settings) {
    this.timestampFunction = timestampFunction;
    this.stimulusFunction = stimulusFunction;
    this.settings = settings;
  }

  @Override
  public L3StreamTupleContainer<ObjectNode> map(ObjectNode value) throws Exception {
    L3StreamTupleContainer<ObjectNode> out = new L3StreamTupleContainer<>(value);
    // out.initGenealog(GenealogTupleType.SOURCE);
    out.setTimestamp(timestampFunction.apply(value));
    out.setStimulus(stimulusFunction.apply(value));
    return out;
  }
}
