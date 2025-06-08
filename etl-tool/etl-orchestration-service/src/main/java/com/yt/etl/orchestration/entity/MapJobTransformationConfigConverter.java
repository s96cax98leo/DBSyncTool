package com.yt.etl.orchestration.entity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yt.etl.common.model.JobTransformationConfig;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

@Converter
public class MapJobTransformationConfigConverter implements AttributeConverter<Map<String, JobTransformationConfig>, String> {

    private static final Logger logger = LoggerFactory.getLogger(MapJobTransformationConfigConverter.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String convertToDatabaseColumn(Map<String, JobTransformationConfig> attribute) {
        if (attribute == null || attribute.isEmpty()) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(attribute);
        } catch (JsonProcessingException e) {
            logger.error("Error converting Map<String, JobTransformationConfig> to JSON string", e);
            throw new IllegalArgumentException("Could not convert attribute to JSON", e);
        }
    }

    @Override
    public Map<String, JobTransformationConfig> convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            return objectMapper.readValue(dbData, new TypeReference<Map<String, JobTransformationConfig>>() {});
        } catch (IOException e) {
            logger.error("Error converting JSON string to Map<String, JobTransformationConfig>", e);
            throw new IllegalArgumentException("Could not convert database data to attribute", e);
        }
    }
}
