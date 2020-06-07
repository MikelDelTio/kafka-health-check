package com.mikeldeltio.kafka.health;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.mikeldeltio.client.IKafkaConstants;
import com.mikeldeltio.client.KafkaManager;

public class KafkaHealthIndicator {

	private final KafkaManager kafkaManager;

	public KafkaHealthIndicator(String bootstrapServers) {
		super();
		this.kafkaManager = new KafkaManager(bootstrapServers);
	}

	public Health getResult() {

		// Declare return variable
		Health health = null;

		try {

			// Create Kafka clients
			AdminClient adminClient = kafkaManager.getAdminClient();
			KafkaConsumer<byte[], byte[]> consumer = kafkaManager.getConsumer();
			KafkaProducer<byte[], byte[]> producer = kafkaManager.getProducer();

			// Get cluster information
			DescribeClusterResult cluster = adminClient.describeCluster(new DescribeClusterOptions().timeoutMs(2000));

			// Extract variables
			String clusterId = cluster.clusterId().get();
			String brokerId = cluster.controller().get().idString();
			int nodes = cluster.nodes().get().size();

			// Verify that the channel works by sending a message to Kafka and reading it
			verifyChannel(producer, consumer);

			// Set health status as UP
			health = Health.up().withDetail("clusterId", clusterId).withDetail("brokerId", brokerId)
					.withDetail("nodes", nodes).build();

		} catch (Throwable ex) {
			// If any error occurs, set health status as DOWN
			return Health.down(ex).build();
		}

		// Return result
		return health;
	}

	private void verifyChannel(KafkaProducer<byte[], byte[]> producer, KafkaConsumer<byte[], byte[]> consumer) {

		// Generate message key
		String msgKey = UUID.randomUUID().toString();

		// Create message
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<byte[], byte[]>(IKafkaConstants.TOPIC_NAME,
				msgKey.getBytes(), IKafkaConstants.MSG.getBytes());

		// Send message without waiting to fill the buffer
		producer.send(record);
		producer.flush();

		// Message readed flag
		boolean msgRecived = false;

		// Attemps flag
		int attempts = 0;

		while (true) {

			// Increase attempts
			attempts++;

			// Get messages or wait the passed timeout if there is none
			final ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(Duration.ofMillis(200));

			// Iterate messages
			Iterator<ConsumerRecord<byte[], byte[]>> consumerRecordIterator = consumerRecords.iterator();
			while (consumerRecordIterator.hasNext()) {

				// Get message
				ConsumerRecord<byte[], byte[]> msg = consumerRecordIterator.next();

				// Search for the message sent by the key
				if (new String(msg.key(), StandardCharsets.UTF_8).equals(msgKey)) {

					// Update message readed flag
					msgRecived = true;

					// Break loop
					break;
				}

			}

			// If no message found count is reached to threshold exit loop
			if (msgRecived || attempts == IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
				break;
			else
				continue;
		}

		// If message not recived, throw error to set health status as DOWN
		if (!msgRecived) {
			throw new RuntimeException("The channel can not be verified");
		}
	}

}
