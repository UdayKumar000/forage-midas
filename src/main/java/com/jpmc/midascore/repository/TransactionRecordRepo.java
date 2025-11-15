package com.jpmc.midascore.repository;

import com.jpmc.midascore.entity.TransactionRecord;
import org.springframework.data.repository.CrudRepository;

public interface TransactionRecordRepo extends CrudRepository<TransactionRecord,Long> {
}
