package io.palyvos.provenance.l3stream.util;

import io.palyvos.provenance.l3stream.util.serializerV2.LineageSerializerLatV2;
import io.palyvos.provenance.l3stream.util.serializerV2.LineageSerializerLinV2;
import io.palyvos.provenance.l3stream.util.serializerV2.LineageSerializerOutV2;
import io.palyvos.provenance.l3stream.util.serializerV2.LineageSerializerV2;
import io.palyvos.provenance.l3stream.wrappers.objects.L3StreamTupleContainer;
import io.palyvos.provenance.util.ExperimentSettings;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.Properties;

public class LineageKafkaSinkV2 implements KafkaSinkStrategyV2 {
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
                    .setRecordSerializer(new LineageSerializerLatV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else if (settings.getLatencyFlag() == 1) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageSerializerV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else if (settings.getLatencyFlag() == 2) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageSerializerLinV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        }  else if (settings.getLatencyFlag() == 100) {
            return KafkaSink.<L3StreamTupleContainer<T>>builder()
                    .setBootstrapServers(broker)
                    .setKafkaProducerConfig(props)
                    .setRecordSerializer(new LineageSerializerOutV2<>(topic, settings))
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();
        } else {
            throw new IllegalArgumentException("LineageKafkaSink");
        }
    }
}
