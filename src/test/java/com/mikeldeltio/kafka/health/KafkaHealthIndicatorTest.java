package com.mikeldeltio.kafka.health;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.mikeldeltio.kafka.health.Health;
import com.mikeldeltio.kafka.health.KafkaHealthIndicator;
import com.mikeldeltio.kafka.health.Status;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;

public class KafkaHealthIndicatorTest {

	@ClassRule
	public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource();

	private static KafkaHealthIndicator kafkaHealthIndicator;

	@BeforeClass
	public static void setup() throws Exception {
		kafkaHealthIndicator = new KafkaHealthIndicator(sharedKafkaTestResource.getKafkaConnectString());
	}

	@Test
	public void testHealthIsUp() throws Exception {
		Health health = kafkaHealthIndicator.getResult();
		assertEquals("Health status is not right", Status.UP, health.getStatus());
		assertEquals("Health details is not right", 3, health.getDetails().size());
		assertEquals("Cluster ID is not right",
				sharedKafkaTestResource.getKafkaTestUtils().getAdminClient().describeCluster().clusterId().get(),
				health.getDetails().get("clusterId"));
		assertEquals("Number of nodes is not right", 1, health.getDetails().get("nodes"));
		assertEquals("Broker ID is not right", "1", health.getDetails().get("brokerId"));
	}

	@Test
	public void testHealthIsDown() throws Exception {
		sharedKafkaTestResource.getKafkaBrokers().getBrokerById(1).stop();
		Health health = kafkaHealthIndicator.getResult();
		assertEquals("Health status is not right", Status.DOWN, health.getStatus());
		assertEquals("Health details is not right", 1, health.getDetails().size());
		assertTrue("Error is not right", health.getDetails().get("error").toString().contains("TimeoutException"));
	}

}
