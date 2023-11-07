package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.l3stream.util.serializer.NonLineageSerializer;
import io.palyvos.provenance.l3stream.util.serializer.NonLineageSerializerLat;
import io.palyvos.provenance.l3stream.util.serializerV2.NonLineageSerializerLatV2;
import io.palyvos.provenance.l3stream.util.serializerV2.NonLineageSerializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class NonLineageKafkaSinkV2 {
    public static <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings) {
        if (settings.getLatencyFlag() == 0) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setRecordSerializer(new NonLineageSerializerLatV2<>(topic))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
            //return new FlinkKafkaProducer<>(topic, new NonLineageSerializerLatV2<>(topic), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        } else if (settings.getLatencyFlag() == 1) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setRecordSerializer(new NonLineageSerializerV2<>(topic))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
            // return new FlinkKafkaProducer<>(topic, new NonLineageSerializerV2<>(topic), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        } else {
            throw new IllegalArgumentException("NonLineageKafkaSink");
        }
    }
}
