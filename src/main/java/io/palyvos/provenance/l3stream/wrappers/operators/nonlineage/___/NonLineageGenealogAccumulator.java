package io.palyvos.provenance.l3stream.wrappers.operators.nonlineage.___;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;
import io.palyvos.provenance.l3stream.wrappers.operators.___.GenealogAccumulatorStrategy;

import java.io.Serializable;

public class NonLineageGenealogAccumulator<T> implements GenealogAccumulatorStrategy, Serializable {
    private final ProvenanceAggregateStrategy strategy;
    T accumulator;
    private long timestamp;
    private long stimulus;
    private boolean lineageReliable = false;

    public NonLineageGenealogAccumulator(ProvenanceAggregateStrategy strategy, T accumulator) {
        this.strategy = strategy;
        this.accumulator = accumulator;
    }

    @Override
    public ProvenanceAggregateStrategy getProvenanceAggregateStrategy() {
        return this.strategy;
    }

    @Override
    public T getAccumulator() {
        return this.accumulator;
    }

    @Override
    public void setAccumulator(Object accumulator) {
        // CNFM
        this.accumulator = (T) accumulator;
    }

    @Override
    public void updateTimestamp(long timestamp) {
        this.timestamp = Math.max(this.timestamp, timestamp);
    }

    @Override
    public void updateStimulus(long stimulus) {
            this.stimulus = Math.max(this.stimulus, stimulus);
        }

    @Override
    public void updateLineageReliable(boolean lineageReliable) {
        this.lineageReliable = this.lineageReliable && lineageReliable;
    }

    @Override
    public boolean lineageReliableResult() {
        return this.lineageReliable ^ false;
    }
}
