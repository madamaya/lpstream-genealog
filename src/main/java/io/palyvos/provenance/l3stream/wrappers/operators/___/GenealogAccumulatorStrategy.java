package io.palyvos.provenance.l3stream.wrappers.operators.___;

import io.palyvos.provenance.ananke.aggregate.ProvenanceAggregateStrategy;

public interface GenealogAccumulatorStrategy<T> {
    public ProvenanceAggregateStrategy getProvenanceAggregateStrategy();

    public T getAccumulator();

    public void setAccumulator(T accumulator);

    public void updateTimestamp(long timestamp);

    public void updateStimulus(long stimulus);

    public void updateLineageReliable(boolean lineageReliable);

    public boolean lineageReliableResult();
}
