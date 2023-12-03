package io.palyvos.provenance.l3stream.wrappers.operators;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;

import java.io.Serializable;
import java.util.List;

/* Modifications copyright (C) 2023 Masaya Yamada */
public class GenealogAccumulator<T> implements Serializable {
    private final ProvenanceAggregateStrategy strategy;
    T accumulator;
    private long timestamp;
    private long stimulus;
    private List<Long> stimulusList;
    private boolean lineageReliable;

    public GenealogAccumulator(ProvenanceAggregateStrategy strategy, T accumulator, boolean lineageReliable) {
        this.strategy = strategy;
        this.accumulator = accumulator;
        this.lineageReliable = lineageReliable;
        stimulusList = null;
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

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    public List<Long> getStimulusList() {
        return this.stimulusList;
    }

    public void setStimulusList(List<Long> stimulusList) {
        this.stimulusList = stimulusList;
    }

    public void updateLineageReliable(boolean lineageReliable) {
        this.lineageReliable = this.lineageReliable && lineageReliable;
    }

    public boolean isLineageReliable() {
        return this.lineageReliable;
    }
}
