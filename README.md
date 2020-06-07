# Kafka Health Check
Health check for microservices that use Apache Kafka by checking whether:

* Connectivity: Cluster information is read to verify that Kafka is UP
* Channel: A message is inserted and read later in a dedicated health check topic to verify that channel still works. 

## Requirements
For the library to work, there needs to exist a topic called "MSHealthCheck" in Kafka.

## Usage
Usage of kafka-health-check:

```java
public class Main {

	public static void main(String[] args) {

		// Define Kafka bootstrap server address
		String bootstrapServers = "127.0.0.1:9092";

		// Create KafkaHealthIndicator
		KafkaHealthIndicator kafkaHealthIndicator = new KafkaHealthIndicator(bootstrapServers);

		// Execute health check
		Health health = kafkaHealthIndicator.getResult();

		// Print result
		if (health.getStatus().equals(Status.UP)) {
			System.out.println("Kafka is UP");
		} else {
			System.out.println("Kafka is DOWN");
		}

	}

}
```
### Relevant Articles

- [Kubernetes: Health Check for a Kafka application](http://mikeldeltio.com/?p=1331) 
- [Distributed Databases: Kafka](http://mikeldeltio.com/?p=1147)
