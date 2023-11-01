package io.palyvos.provenance.l3stream.wrappers.objects;

public class L3StreamInput<T> {
    private int partitionID;
    private long stimulus = Long.MIN_VALUE;
    private T value;

    public L3StreamInput(int partitionID, T value, long stimulus) {
        this.partitionID = partitionID;
        this.value = value;
        this.stimulus = stimulus;
    }

    public L3StreamInput(int partitionID, T value) {
        this.partitionID = partitionID;
        this.value = value;
    }

    public L3StreamInput(T value, long stimulus) {
        this.value = value;
        this.stimulus = stimulus;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    public long getStimulus() {
        return stimulus;
    }

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "L3StreamInput{" +
                "value=" + value +
                '}';
    }
}
