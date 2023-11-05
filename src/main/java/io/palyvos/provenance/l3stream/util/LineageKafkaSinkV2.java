package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.l3stream.util.serializer.LineageSerializer;
import io.palyvos.provenance.l3stream.util.serializer.LineageSerializerLat;
import io.palyvos.provenance.l3stream.util.serializer.LineageSerializerLin;
import io.palyvos.provenance.l3stream.util.serializerV2.LineageSerializerLatV2;
import io.palyvos.provenance.l3stream.util.serializerV2.LineageSerializerLinV2;
import io.palyvos.provenance.l3stream.util.serializerV2.LineageSerializerV2;
import io.palyvos.provenance.l3stream.util.serializerV2.NonLineageSerializerLatV2;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class LineageKafkaSinkV2 {
    public static <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings) {
        if (settings.getLatencyFlag() == 0) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setRecordSerializer(new LineageSerializerLatV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
            // return new FlinkKafkaProducer<>(topic, new LineageSerializerLat<>(topic, settings), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        } else if (settings.getLatencyFlag() == 1) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setRecordSerializer(new LineageSerializerV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
            // return new FlinkKafkaProducer<>(topic, new LineageSerializer<>(topic, settings), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        } else if (settings.getLatencyFlag() == 2) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setRecordSerializer(new LineageSerializerLinV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                    .build();
            // return new FlinkKafkaProducer<>(topic, new LineageSerializerLin<>(topic, settings), prop, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        } else {
            throw new IllegalArgumentException("LineageKafkaSink");
        }
    }
}