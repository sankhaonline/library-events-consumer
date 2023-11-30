package io.sankha.scheduler;

import io.sankha.config.LibraryEventsConsumerConfig;
import io.sankha.entity.FailureRecord;
import io.sankha.jpa.FailureRecordRepository;
import io.sankha.service.LibraryEventsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class RetryScheduler {

  private final LibraryEventsService libraryEventsService;
  private final FailureRecordRepository failureRecordRepository;

  @Scheduled(fixedRate = 10000)
  public void retryFailedRecords() {

    log.info("Retrying Failed Records Started!");
    var status = LibraryEventsConsumerConfig.RETRY;
    failureRecordRepository
        .findAllByStatus(status)
        .forEach(
            failureRecord -> {
              try {
                // libraryEventsService.processLibraryEvent();
                var consumerRecord = buildConsumerRecord(failureRecord);
                libraryEventsService.processLibraryEvent(consumerRecord);
                // libraryEventsConsumer.onMessage(consumerRecord); // This does not involve the
                // recovery code for in the consumerConfig
                failureRecord.setStatus(LibraryEventsConsumerConfig.SUCCESS);
              } catch (Exception exception) {
                log.error("Exception in retryFailedRecords : ", exception);
              }
            });
  }

  private ConsumerRecord<Integer, String> buildConsumerRecord(FailureRecord failureRecord) {

    return new ConsumerRecord<>(
        failureRecord.getTopic(),
        failureRecord.getPartition(),
        failureRecord.getOffset_value(),
        failureRecord.getKey_value(),
        failureRecord.getErrorRecord());
  }
}
