package io.palyvos.provenance.l3stream.util.deserializerV2;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInput;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class JsonNodeGLDeserializerV2 implements KafkaRecordDeserializationSchema<L3StreamInput<JsonNode>> {
    final private ObjectMapper om;

    public JsonNodeGLDeserializerV2() {
        om = new ObjectMapper();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<L3StreamInput<JsonNode>> collector) throws IOException {
        long stimulus = System.nanoTime();
        collector.collect(new L3StreamInput<>(om.readTree(new String(consumerRecord.value())), stimulus));
    }

    @Override
    public TypeInformation<L3StreamInput<JsonNode>> getProducedType() {
        return TypeInformation.of(new TypeHint<>(){});
    }
}
