package io.sankha.config;

import io.sankha.service.FailureService;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
@EnableKafka
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsConsumerConfig {

  public static final String RETRY = "RETRY";
  public static final String SUCCESS = "SUCCESS";
  public static final String DEAD = "DEAD";

  private final KafkaProperties kafkaProperties;

  private final KafkaTemplate kafkaTemplate;

  private final FailureService failureService;

  @Value("${topics.retry:library-events.RETRY}")
  private String retryTopic;

  @Value("${topics.dlt:library-events.DLT}")
  private String deadLetterTopic;

  public DeadLetterPublishingRecoverer publishingRecoverer() {

    return new DeadLetterPublishingRecoverer(
        kafkaTemplate,
        (records, exception) -> {
          log.error("Exception in publishingRecoverer : {} ", exception.getMessage(), exception);
          if (exception.getCause() instanceof RecoverableDataAccessException) {
            return new TopicPartition(retryTopic, records.partition());
          } else {
            return new TopicPartition(deadLetterTopic, records.partition());
          }
        });
  }

  ConsumerRecordRecoverer consumerRecordRecoverer =
      (records, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, records);
        if (exception.getCause() instanceof RecoverableDataAccessException) {
          log.info("Inside the recoverable logic");
          // Add any Recovery Code here.
          failureService.saveFailedRecord(
              (ConsumerRecord<Integer, String>) records, exception, RETRY);

        } else {
          log.info("Inside the non recoverable logic and skipping the record : {}", records);
        }
      };

  @Bean
  ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
      ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
      ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
    var factory = new ConcurrentKafkaListenerContainerFactory<>();
    configurer.configure(
        factory,
        kafkaConsumerFactory.getIfAvailable(
            () ->
                new DefaultKafkaConsumerFactory<>(
                    this.kafkaProperties.buildConsumerProperties(null))));
    factory.setConcurrency(3);
    factory.setCommonErrorHandler(errorHandler());
    return factory;
  }

  public DefaultErrorHandler errorHandler() {
    var exceptiopnToIgnorelist = List.of(IllegalArgumentException.class);

    var expBackOff = new ExponentialBackOffWithMaxRetries(2);
    expBackOff.setInitialInterval(1_000L);
    expBackOff.setMultiplier(2.0);
    expBackOff.setMaxInterval(2_000L);

    // var fixedBackOff = new FixedBackOff(1000L, 2L);

    var defaultErrorHandler =
        new DefaultErrorHandler(
            publishingRecoverer(),
            // fixedBackOff
            expBackOff);

    exceptiopnToIgnorelist.forEach(defaultErrorHandler::addNotRetryableExceptions);

    defaultErrorHandler.setRetryListeners(
        (records, ex, deliveryAttempt) ->
            log.info(
                "Failed Record in Retry Listener exception : {} , deliveryAttempt : {} ",
                ex.getMessage(),
                deliveryAttempt));
    return defaultErrorHandler;
  }
}
