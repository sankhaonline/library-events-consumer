package io.sankha.jpa;

import io.sankha.entity.FailureRecord;
import java.util.List;
import org.springframework.data.repository.CrudRepository;

public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {

  List<FailureRecord> findAllByStatus(String status);
}
