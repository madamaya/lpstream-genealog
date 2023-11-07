package io.palyvos.provenance.l3stream.util.deserializerV2.old;

import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamInputOld;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class StringL3DeserializerV2 implements KafkaRecordDeserializationSchema<L3StreamInputOld<String>> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<L3StreamInputOld<String>> collector) throws IOException {
        long stimulus = System.nanoTime();
        collector.collect(new L3StreamInputOld<>(consumerRecord.partition(), new String(consumerRecord.value()), stimulus));
    }

    @Override
    public TypeInformation<L3StreamInputOld<String>> getProducedType() {
        return TypeInformation.of(new TypeHint<>(){});
    }
}

