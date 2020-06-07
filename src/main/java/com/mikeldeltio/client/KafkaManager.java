package com.mikeldeltio.client;

import static com.mikeldeltio.kafka.util.MapUtils.createMap;
import static com.mikeldeltio.kafka.util.MapUtils.entry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaManager {

	private final Logger LOGGER = LoggerFactory.getLogger(getClass());

	private final AdminClient adminClient;

	private final String bootstrapServers;

	private final KafkaConsumer<byte[], byte[]> consumer;

	private final KafkaProducer<byte[], byte[]> producer;

	public KafkaManager(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
		this.producer = createProducer();
		this.consumer = createConsumer();
		this.adminClient = createAdminClient();
	}

	public AdminClient getAdminClient() {
		return this.adminClient;
	}

	public KafkaConsumer<byte[], byte[]> getConsumer() {
		return consumer;
	}

	public KafkaProducer<byte[], byte[]> getProducer() {
		return this.producer;
	}

	private AdminClient createAdminClient() {
		// Declare admin client
		AdminClient adminClient = null;

		try {
			// Set admin client configuration properties
			Map<String, Object> props = createMap(entry(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));

			// Create and return admin client
			adminClient = AdminClient.create(props);
		} catch (Exception e) {
			LOGGER.info("Connection to Kafka could not be established");
		}

		// Return admin client
		return adminClient;
	}

	private KafkaConsumer<byte[], byte[]> createConsumer() {
		// Set Kafka consumer configuration properties
		Map<String, Object> props = createMap(entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
				entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, IKafkaConstants.OFFSET_RESET_LATEST),
				entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class),
				entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class));

		// Declare Kafka consumer
		KafkaConsumer<byte[], byte[]> kafkaConsumer = null;

		try {
			// Create Kafka consumer
			kafkaConsumer = new KafkaConsumer<>(props);

			// Determine which partitions to subscribe to, for now do all
			final List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(IKafkaConstants.TOPIC_NAME);

			// Check if topic exist
			if (partitionInfos != null) {

				// Pull out partitions, convert to topic partitions
				final Collection<TopicPartition> topicPartitions = new ArrayList<>();
				for (final PartitionInfo partitionInfo : partitionInfos) {
					topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
				}

				// Asign topic partitions to consumer
				kafkaConsumer.assign(topicPartitions);

				// Poll to initialize offset
				kafkaConsumer.poll(Duration.ofMillis(1000));

			} else {
				LOGGER.info("{} topic does not exist and health check tests will fail", IKafkaConstants.TOPIC_NAME);
			}
		} catch (Exception e) {
			LOGGER.info("Connection to Kafka could not be established");
		}

		// Return Kafka consumer
		return kafkaConsumer;
	}

	private KafkaProducer<byte[], byte[]> createProducer() {
		// Declare Kafka producer
		KafkaProducer<byte[], byte[]> kafkaProducer = null;

		try {
			// Set Kafka producer configuration properties
			Map<String, Object> props = createMap(entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
					entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()),
					entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName()));

			// Create and return Kafka producer
			kafkaProducer = new KafkaProducer<>(props);
		} catch (Exception e) {
			LOGGER.info("Connection to Kafka could not be established");
		}

		// Return Kafka producer
		return kafkaProducer;
	}

}
