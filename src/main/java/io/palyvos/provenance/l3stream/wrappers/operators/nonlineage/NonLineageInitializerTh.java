package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage;

import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class NonLineageInitializerTh extends RichMapFunction<ObjectNode, L3StreamTupleContainer<ObjectNode>> {
  long start;
  long count;
  ExperimentSettings settings;
  int sourceID;

  public <F extends Function<ObjectNode, Long> & Serializable> NonLineageInitializerTh(ExperimentSettings settings, int sourceID) {
    this.settings = settings;
    this.sourceID = sourceID;
  }

  @Override
  public L3StreamTupleContainer<ObjectNode> map(ObjectNode value) throws Exception {
    L3StreamTupleContainer<ObjectNode> out = new L3StreamTupleContainer<>(value);
    // out.initGenealog(GenealogTupleType.SOURCE);
    //out.setTimestamp(System.currentTimeMillis());
    out.setStimulus(System.nanoTime());
    count++;
    return out;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    start = System.nanoTime();
    count = 0L;
  }

  @Override
  public void close() throws Exception {
    long end = System.nanoTime();

    String dataPath = L3conf.L3_HOME + "/data/output/throughput/" + settings.getQueryName();
    if (Files.notExists(Paths.get(dataPath))) {
      Files.createDirectories(Paths.get(dataPath));
    }

    PrintWriter pw = new PrintWriter(dataPath + "/" + settings.getStartTime() + "_" + sourceID + "_" + getRuntimeContext().getIndexOfThisSubtask() + ".log");
    pw.println(start + "," + end + "," + (end - start) + "," + count);
    pw.flush();
    pw.close();
    super.close();
  }
}
