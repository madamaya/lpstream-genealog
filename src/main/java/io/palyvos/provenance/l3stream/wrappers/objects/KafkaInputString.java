package io.palyvos.provenance.l3stream.wrappers.objects;

public class KafkaInputString extends KafkaInput {
    private String str;

    public KafkaInputString(int partitionID, String str, long stimulus) {
        super(partitionID, stimulus);
        this.str = str;
    }

    public KafkaInputString(int partitionID, String str) {
        super(partitionID);
        this.str = str;
    }

    public KafkaInputString(String str, long stimulus) {
        super(stimulus);
        this.str = str;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    @Override
    public String toString() {
        return "\"" + str + "\"";
    }
}
