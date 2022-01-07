
package com.example;

import com.common.Book;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.util.backoff.FixedBackOff;

	@SpringBootApplication
	public class BookApiConsumerKafkaApplication {

		private final Logger logger = LoggerFactory.getLogger(BookApiConsumerKafkaApplication.class);

		public static void main(String[] args) {
			SpringApplication.run(BookApiConsumerKafkaApplication.class, args);
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
				ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
				ConsumerFactory<Object, Object> kafkaConsumerFactory,
				KafkaTemplate<Object, Object> template) {
			ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
			configurer.configure(factory, kafkaConsumerFactory);
			factory.setErrorHandler(new SeekToCurrentErrorHandler(
					new DeadLetterPublishingRecoverer(template), new FixedBackOff(5000L, 3))); // dead-letter after 3 tries
			return factory;
		}

		@Bean
		public RecordMessageConverter converter() {
			return new StringJsonMessageConverter();
		}

		Book storedBook;

		Message<Book> storedBookMessage;

		@KafkaListener(id = "fooGroup", topics = "topic1")
		public void listen(Message<Book> fooMsg) {
			Book book = fooMsg.getPayload();
			logger.info("Received: " + book);
			if (book.getBookName().startsWith("fail")) {
				throw new RuntimeException("failed");
			}
			this.storedBook = book;
			this.storedBookMessage = fooMsg;
		}

		@KafkaListener(id = "dltGroup", topics = "topic1.DLT")
		public void dltListen(String in) {
			logger.info("Received from DLT: " + in);
		}

		@Bean
		public NewTopic topic() {
			return new NewTopic("topic1", 1, (short) 1);
		}

		@Bean
		public NewTopic dlt() {
			return new NewTopic("topic1.DLT", 1, (short) 1);
		}

	}
