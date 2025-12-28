package com.smith.processor.service;

import com.smith.processor.DAO.EventFailedRepository;
import com.smith.processor.DAO.EventRepository;
import com.smith.processor.DAO.TechnicalFailureRepository;
import com.smith.processor.config.ProcessorMapper;
import com.smith.processor.dto.EventDTO;
import com.smith.processor.entity.EventEntity;
import com.smith.processor.entity.EventFailedEntity;
import com.smith.processor.entity.TechnicalFailureEntity;
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

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private TechnicalFailureRepository technicalFailureRepository;

    public void saveEventFailed(EventDTO dto, KafkaFailedData failedData){

        logger.info("Saving failed event with id: {}", dto.getId());

        EventFailedEntity entity = ProcessorMapper.toEntity(dto,failedData);

        logger.debug("Saving failed event: {}", entity);

        eventFailedRepository.save(entity);

        logger.info("Event: {} saved", entity.getId_event());
    }

    public void saveProcessedEvents(EventDTO dto,String topic,long offset,int partition,String groupId){
        logger.info("Saving processed id event: {}",dto.getId());

        EventEntity entity = ProcessorMapper.toEntity(dto,topic,offset,partition, groupId);

        logger.debug("Saving entity: {}", entity);

        eventRepository.save(entity);

        logger.info("saved id event: {}",dto.getId());
    }

    public void saveTechnicalFailure(EventDTO dto,String topic, int partition, long offset, String stacktrace){

        logger.info("saving technical failure for event: {}", dto.getId());

        TechnicalFailureEntity entity = ProcessorMapper.toFailureEntity(dto,topic,offset,partition,stacktrace);
        technicalFailureRepository.save(entity);

        logger.info("technical failure for event: {}, saved",dto.getId());
    }
}
