package io.palyvos.provenance.l3stream.util.deserializerV2.old;

import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputJsonNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class JsonNodeDeserializerV2 implements KafkaRecordDeserializationSchema<KafkaInputJsonNode> {
    final private ObjectMapper om;

    public JsonNodeDeserializerV2() {
        om = new ObjectMapper();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaInputJsonNode> collector) throws IOException {
        long stimulus = System.nanoTime();
        collector.collect(new KafkaInputJsonNode(consumerRecord.partition(), om.readTree(new String(consumerRecord.value())), stimulus));
    }

    @Override
    public TypeInformation<KafkaInputJsonNode> getProducedType() {
        return TypeInformation.of(KafkaInputJsonNode.class);
    }
}
