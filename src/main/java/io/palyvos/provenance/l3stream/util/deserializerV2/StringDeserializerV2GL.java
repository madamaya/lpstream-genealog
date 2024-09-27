package io.palyvos.provenance.l3stream.util.deserializerV2;

import io.palyvos.provenance.genealog.GenealogTupleType;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputString;
import io.palyvos.provenance.l3stream.wrappers.objects.KafkaInputStringGL;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

public class StringDeserializerV2GL implements KafkaRecordDeserializationSchema<KafkaInputStringGL> {
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<KafkaInputStringGL> collector) throws IOException {
        long stimulus = System.nanoTime();
        KafkaInputStringGL out = new KafkaInputStringGL(consumerRecord.partition(), new String(consumerRecord.value()), consumerRecord.timestamp(), stimulus);
        out.initGenealog(GenealogTupleType.SOURCE);
        collector.collect(out);
    }

    @Override
    public TypeInformation<KafkaInputStringGL> getProducedType() {
        return TypeInformation.of(KafkaInputStringGL.class);
    }
}
