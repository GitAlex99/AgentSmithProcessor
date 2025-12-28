package com.smith.processor.DAO;

import com.smith.processor.entity.TechnicalFailureEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TechnicalFailureRepository extends JpaRepository<TechnicalFailureEntity,Long> {
}
