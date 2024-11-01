package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.l3stream.util.serializerV2.NonLineageSerializerLatV2;
import io.palyvos.provenance.l3stream.util.serializerV2.NonLineageSerializerOutV2;
import io.palyvos.provenance.l3stream.util.serializerV2.NonLineageSerializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.Properties;

public class NonLineageKafkaSinkV2 implements KafkaSinkStrategyV2 {
    @Override
    public <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings) {
        return newInstance(topic, broker, settings, new Properties());
    }

    @Override
    public <T> KafkaSink<L3StreamTupleContainer<T>> newInstance(String topic, String broker, ExperimentSettings settings, Properties props) {
        if (settings.getLatencyFlag() == 0) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new NonLineageSerializerLatV2<>(topic))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else if (settings.getLatencyFlag() == 1) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new NonLineageSerializerV2<>(topic))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else if (settings.getLatencyFlag() == 100) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new NonLineageSerializerOutV2<>(topic))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            throw new IllegalArgumentException("NonLineageKafkaSink");
        }
    }
}
