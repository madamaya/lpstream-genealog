package io.palyvos.provenance.l3stream.wrappers.operators;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.l3stream.util.object.TimestampsForLatency;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class GenealogAccumulator<T> implements Serializable {
    private final ProvenanceAggregateStrategy strategy;
    T accumulator;
    private long timestamp;
    private long stimulus;
    private TimestampsForLatency tfl;
    private boolean lineageReliable;

    public GenealogAccumulator(ProvenanceAggregateStrategy strategy, T accumulator, boolean lineageReliable) {
        this.strategy = strategy;
        this.accumulator = accumulator;
        this.lineageReliable = lineageReliable;
    }

    public ProvenanceAggregateStrategy getStrategy() {
        return strategy;
    }

    public T getAccumulator() {
        return accumulator;
    }

    public void setAccumulator(T accumulator) {
        this.accumulator = accumulator;
    }

    public void updateTimestamp(long timestamp) {
        this.timestamp = Math.max(this.timestamp, timestamp);
    }

    public void updateStimulus(long stimulus) {
        this.stimulus = Math.max(this.stimulus, stimulus);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getStimulus() {
        return stimulus;
    }

    public TimestampsForLatency getTfl() {
        return tfl;
    }

    public void setTfl(TimestampsForLatency tfl) {
        if (this.tfl == null) {
            this.tfl = tfl;
        } else {
            if (this.tfl.ts2 >= tfl.ts2) {
                this.tfl.setTs1(Math.max(this.tfl.ts1, tfl.ts1));
            } else if (this.tfl.ts2 < tfl.ts2) {
                long tmp = Math.max(this.tfl.ts1, tfl.ts1);
                this.tfl = tfl;
                this.tfl.setTs1(tmp);
            } else {
                throw new IllegalStateException();
            }
        }
    }

    public void updateLineageReliable(boolean lineageReliable) {
        this.lineageReliable = this.lineageReliable && lineageReliable;
    }

    public boolean isLineageReliable() {
        return this.lineageReliable;
    }
}
