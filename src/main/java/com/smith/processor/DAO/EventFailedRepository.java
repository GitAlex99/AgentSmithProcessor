package com.smith.processor.DAO;

import com.smith.processor.entity.EventFailedEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EventFailedRepository extends JpaRepository<EventFailedEntity, Long> {
}
