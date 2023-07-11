package io.palyvos.provenance.l3stream.wrappers.operators;

import io.palyvos.provenance.l3stream.util.L3Settings;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.io.PrintWriter;

/* Add copyright (C) 2023 Masaya Yamada */
public class CountInput<T> extends RichMapFunction<L3StreamTupleContainer<T>, L3StreamTupleContainer<T>> {
    long start;
    long count;
    L3Settings settings;

    public CountInput(L3Settings settings) {
        this.settings = settings;
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
        PrintWriter pw = new PrintWriter(System.getenv("HOME") + "/logs/win_baseline/throughput" + end + ".log");
        pw.println(start + "," + end + "," + (end - start) + "," + count);
        pw.flush();
        pw.close();
        super.close();
    }

    @Override
    public L3StreamTupleContainer<T> map(L3StreamTupleContainer<T> value) throws Exception {
        count++;
        return new L3StreamTupleContainer<>(value);
    }
}
