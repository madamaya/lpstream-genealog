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
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class LineageKafkaSinkV2 implements KafkaSinkStrategyV2 {
    @Override
    public <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings) {
        if (settings.getLatencyFlag() == 0) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setRecordSerializer(new LineageSerializerLatV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else if (settings.getLatencyFlag() == 1) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setRecordSerializer(new LineageSerializerV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else if (settings.getLatencyFlag() == 2) {
            Properties props = new Properties();
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 3200000);
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageSerializerLinV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            throw new IllegalArgumentException("LineageKafkaSink");
        }
    }
}
