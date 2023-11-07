package io.palyvos.provenance.l3stream.wrappers.objects;

public class KafkaInput {
    private int partitionID;
    private long stimulus = Long.MIN_VALUE;

    public KafkaInput(int partitionID, long stimulus) {
        this.partitionID = partitionID;
        this.stimulus = stimulus;
    }

    public KafkaInput(int partitionID) {
        this.partitionID = partitionID;
    }

    public KafkaInput(long stimulus) {
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
}
