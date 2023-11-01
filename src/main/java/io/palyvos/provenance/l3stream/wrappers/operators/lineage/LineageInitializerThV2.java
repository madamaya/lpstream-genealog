package io.palyvos.provenance.l3stream.wrappers.operators.lineage;

import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.l3stream.conf.L3conf;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInput;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;

/* Modifications copyright (C) 2023 Masaya Yamada */

public class LineageInitializerThV2<T> extends RichMapFunction<L3StreamInput<T>, L3StreamTupleContainer<L3StreamInput<T>>> {
  long start;
  long count;
  ExperimentSettings settings;
  int sourceID;

  public LineageInitializerThV2(ExperimentSettings settings, int sourceID) {
    this.settings = settings;
    this.sourceID = sourceID;
  }

  @Override
  public L3StreamTupleContainer<L3StreamInput<T>> map(L3StreamInput<T> value) throws Exception {
    L3StreamTupleContainer<L3StreamInput<T>> out = new L3StreamTupleContainer<>(value);
    out.initGenealog(GenealogTupleType.SOURCE);
    out.setLineageReliable(true);
    //out.setTimestamp(System.currentTimeMillis());
    //out.setTimestamp(timestampFunction.apply(value));
    out.setStimulus(System.nanoTime());
    //out.setStimulus(stimulusFunction.apply(value));
    out.setPartitionId(value.getPartitionID());
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
