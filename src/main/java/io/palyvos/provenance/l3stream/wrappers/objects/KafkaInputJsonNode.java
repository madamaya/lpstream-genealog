package io.palyvos.provenance.l3stream.wrappers.objects;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

public class KafkaInputJsonNode extends KafkaInput {
    private JsonNode jsonNode;

    public KafkaInputJsonNode(int partitionID, JsonNode jsonNode, long stimulus) {
        super(partitionID, stimulus);
        this.jsonNode = jsonNode;
    }

    public KafkaInputJsonNode(int partitionID, JsonNode jsonNode) {
        super(partitionID, Long.MIN_VALUE);
        this.jsonNode = jsonNode;
    }

    public KafkaInputJsonNode(JsonNode jsonNode, long stimulus) {
        super(Long.MIN_VALUE, stimulus);
        this.jsonNode = jsonNode;
    }

    public JsonNode getJsonNode() {
        return jsonNode;
    }

    public void setJsonNode(JsonNode jsonNode) {
        this.jsonNode = jsonNode;
    }

    @Override
    public String toString() {
        return "KafkaInputJsonNode{" +
                "jsonNode=" + jsonNode +
                '}';
    }
}
