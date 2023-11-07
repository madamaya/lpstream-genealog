package io.palyvos.provenance.l3stream.util.deserializerV2.old;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInputOld;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class JsonNodeL3DeserializerV2 implements KafkaRecordDeserializationSchema<L3StreamInputOld<JsonNode>> {
    final private ObjectMapper om;

    public JsonNodeL3DeserializerV2() {
        om = new ObjectMapper();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<L3StreamInputOld<JsonNode>> collector) throws IOException {
        long stimulus = System.nanoTime();
        collector.collect(new L3StreamInputOld<>(consumerRecord.partition(), om.readTree(new String(consumerRecord.value())), stimulus));
    }

    @Override
    public TypeInformation<L3StreamInputOld<JsonNode>> getProducedType() {
        return TypeInformation.of(new TypeHint<>(){});
    }
}
