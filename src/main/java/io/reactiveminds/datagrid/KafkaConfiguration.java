package io.reactiveminds.datagrid;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;

import com.hazelcast.internal.serialization.impl.ConstantSerializers.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.reactiveminds.datagrid.core.RequestConsumer;

@Configuration
public class KafkaConfiguration {

	@Value("${coalesce.kafka.bootstrapServers:localhost:9092}")
	private String bootstrapServers;
	@Value("${coalesce.kafka.schemaRegistryUrl:localhost:8081}")
	private String schemaRegistryUrl;
	@Value("${coalesce.kafka.consumerConcurrency:1}")
	private int concurrency;
	@Bean
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	RequestConsumer requestConsumer(String imap) {
		return new RequestConsumer(imap);
	}
	@Bean
	KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<GenericRecord, GenericRecord>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<GenericRecord, GenericRecord> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(concurrency);
		factory.getContainerProperties().setPollTimeout(3000);
		return factory;
	}
	/**
	 * 
	 * @param topic
	 * @param requestMap
	 * @param groupId
	 * @return
	 */
	@Bean(destroyMethod = "stop")
	@Scope(scopeName = ConfigurableBeanFactory.SCOPE_PROTOTYPE)
	@Lazy
	MessageListenerContainer kafkaConsumer(String topic, String requestMap) {
	    ConcurrentMessageListenerContainer<GenericRecord, GenericRecord> container = kafkaListenerContainerFactory().createContainer(topic);
	    container.setupMessageListener(requestConsumer(requestMap));
	    container.setBeanName("kafkaConsumer."+requestMap);
	    container.setConcurrency(concurrency);
	    container.getContainerProperties().setGroupId(topic+"_"+requestMap);
	    return container;
	}

    @Bean
    public ConsumerFactory<GenericRecord, GenericRecord> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs());
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		props.put("schema.registry.url", schemaRegistryUrl);   
		props.put("specific.avro.reader", true);
		return props;
    }
    
	@Bean
	public KafkaTemplate<String, String> eventsKafkaTemplate(){
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(props);
		return new KafkaTemplate<>(producerFactory);
	}
}
