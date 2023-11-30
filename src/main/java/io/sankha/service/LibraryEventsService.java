package io.sankha.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.sankha.entity.LibraryEvent;
import io.sankha.jpa.LibraryEventsRepository;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class LibraryEventsService {
  private final ObjectMapper objectMapper;

  private final LibraryEventsRepository repository;

  public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord)
      throws JsonProcessingException {
    var libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
    log.info("libraryEvent : {}", libraryEvent);

    if (null != libraryEvent.getLibraryEventId() && (999 == libraryEvent.getLibraryEventId())) {
      throw new RecoverableDataAccessException("Temporary Network Issue");
    }

    switch (libraryEvent.getLibraryEventType()) {
      case NEW -> save(libraryEvent);
      case UPDATE -> {
        validate(libraryEvent);
        save(libraryEvent);
      }
      default -> log.info("Invalid libraryEventType");
    }
  }

  private void validate(LibraryEvent libraryEvent) {
    var libraryEventId =
        Optional.ofNullable(libraryEvent.getLibraryEventId())
            .orElseThrow(() -> new IllegalArgumentException("Library Event Id is missing"));
    Optional.of(repository.findById(libraryEventId))
        .orElseThrow(() -> new IllegalArgumentException("Not a valid Library Event"))
        .ifPresent(
            libraryEventOp ->
                log.info("Validation is successful for the Library Event : {}", libraryEventOp));
  }

  private void save(LibraryEvent libraryEvent) {
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    repository.save(libraryEvent);
    log.info("Successfully Persisted the library Event : {}", libraryEvent);
  }
}
