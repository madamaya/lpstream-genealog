package io.palyvos.provenance.l3stream.util.deserializerV2;

import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class StringDeserializerV2 implements KafkaRecordDeserializationSchema<KafkaInputString> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaInputString> collector) throws IOException {
        long stimulus = System.nanoTime();
        collector.collect(new KafkaInputString(consumerRecord.partition(), new String(consumerRecord.value()), stimulus));
    }

    @Override
    public TypeInformation<KafkaInputString> getProducedType() {
        return TypeInformation.of(KafkaInputString.class);
    }
}
