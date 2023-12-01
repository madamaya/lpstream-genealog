package io.palyvos.provenance.l3stream.wrappers.objects;

public class KafkaInput {
    private int partitionID;
    private long kafkaAppandTime = Long.MIN_VALUE;
    private long stimulus = Long.MIN_VALUE;

    public KafkaInput(int partitionID, long kafkaAppandTime, long stimulus) {
        this.partitionID = partitionID;
        this.kafkaAppandTime = kafkaAppandTime;
        this.stimulus = stimulus;
    }

    public KafkaInput(int partitionID, long kafkaAppandTime) {
        this.partitionID = partitionID;
        this.kafkaAppandTime = kafkaAppandTime;
    }

    public KafkaInput(long kafkaAppandTime, long stimulus) {
        this.kafkaAppandTime = kafkaAppandTime;
        this.stimulus = stimulus;
    }

    public int getPartitionID() {
        return partitionID;
    }

    public void setPartitionID(int partitionID) {
        this.partitionID = partitionID;
    }

    public long getKafkaAppandTime() {
        return kafkaAppandTime;
    }

    public void setKafkaAppandTime(long kafkaAppandTime) {
        this.kafkaAppandTime = kafkaAppandTime;
    }

    public long getStimulus() {
        return stimulus;
    }

    public void setStimulus(long stimulus) {
        this.stimulus = stimulus;
    }
}
