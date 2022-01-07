
package com.example;

import org.assertj.core.api.BDDAssertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import org.springframework.cloud.contract.stubrunner.StubTrigger;
import org.springframework.cloud.contract.stubrunner.spring.AutoConfigureStubRunner;
import org.springframework.cloud.contract.stubrunner.spring.StubRunnerProperties;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@AutoConfigureStubRunner(ids = "com.example:book-api-producer-kafka", stubsMode = StubRunnerProperties.StubsMode.LOCAL)
@EmbeddedKafka(topics = "topic1")
@ActiveProfiles("test")
public class BookApiConsumerKafkaApplicationTests {


	@Autowired
	StubTrigger trigger;
	@Autowired
	BookApiConsumerKafkaApplication application;

	@Test
	public void consumesBookAsObject() {
		this.trigger.trigger("trigger");

		Awaitility.await().untilAsserted(() -> {
			BDDAssertions.then(this.application.storedBook).isNotNull();
			BDDAssertions.then(this.application.storedBook.getBookName()).contains("GODFATHER");
		});
	}

	@Test
	public void consumesBookAsMessage() {
		this.trigger.trigger("triggerMessage");

		Awaitility.await().untilAsserted(() -> {
			BDDAssertions.then(this.application.storedBookMessage).isNotNull();
			BDDAssertions.then(this.application.storedBookMessage.getPayload().getBookName()).contains("CRIMEANDPUNISHMENT");
			BDDAssertions.then(this.application.storedBookMessage.getHeaders().containsKey("kafka_receivedMessageKey"));
			BDDAssertions.then(this.application.storedBookMessage.getHeaders().get("kafka_receivedMessageKey"))
					.isEqualTo("key-example");
		});
	}


}

