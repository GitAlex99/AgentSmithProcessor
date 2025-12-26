package com.smith.processor.service;

import com.smith.processor.DAO.EventFailedRepository;
import com.smith.processor.config.ProcessorMapper;
import com.smith.processor.dto.EventDTO;
import com.smith.processor.entity.EventFailedEntity;
import com.smith.processor.listener.EventListener;
import com.smith.processor.model.KafkaFailedData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class EventService {

    private static final Logger logger = LoggerFactory.getLogger(EventService.class);

    @Autowired
    private EventFailedRepository eventFailedRepository;

    public void saveEventFailed(EventDTO dto, KafkaFailedData failedData){

        EventFailedEntity entity = ProcessorMapper.toEntity(dto,failedData);

        logger.debug("Saving failed event: {} with failed data: {}", dto,failedData);

        logger.info("Saving failed event with id: {}", entity.getId_event());

        eventFailedRepository.save(entity);

        logger.info("Event: {} saved", entity.getId_event());
    }
}
