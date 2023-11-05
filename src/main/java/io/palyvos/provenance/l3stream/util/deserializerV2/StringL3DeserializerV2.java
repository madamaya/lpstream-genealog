package io.palyvos.provenance.l3stream.util.deserializerV2;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInput;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class StringL3DeserializerV2 implements KafkaRecordDeserializationSchema<L3StreamInput<String>> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<L3StreamInput<String>> collector) throws IOException {
        long stimulus = System.nanoTime();
        collector.collect(new L3StreamInput<>(consumerRecord.partition(), new String(consumerRecord.value()), stimulus));
    }

    @Override
    public TypeInformation<L3StreamInput<String>> getProducedType() {
        return TypeInformation.of(new TypeHint<>(){});
    }
}

