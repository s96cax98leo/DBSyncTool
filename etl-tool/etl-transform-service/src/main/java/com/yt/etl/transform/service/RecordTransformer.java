package com.yt.etl.transform.service;

import com.yt.etl.common.model.JobTransformationConfig;
import com.yt.etl.common.model.TransformationRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.math.BigDecimal;

@Service
public class RecordTransformer {
    private static final Logger logger = LoggerFactory.getLogger(RecordTransformer.class);

    public List<Map<String, Object>> transformRecords(List<Map<String, Object>> originalRecords, JobTransformationConfig config) {
        if (config == null || config.getRules() == null || config.getRules().isEmpty()) {
            logger.warn("No transformation rules provided. Returning original records.");
            return originalRecords;
        }

        List<Map<String, Object>> transformedRecords = new ArrayList<>();
        for (Map<String, Object> originalRecord : originalRecords) {
            Map<String, Object> newRecord = new HashMap<>();
            for (TransformationRule rule : config.getRules()) {
                try {
                    applyRule(originalRecord, newRecord, rule);
                } catch (Exception e) {
                    logger.error("Error applying rule {} for source field '{}' to target field '{}'. Record: {}",
                            rule.getTransformationType(), rule.getSourceField(), rule.getTargetField(), originalRecord, e);
                    // Optionally, decide if the record should be skipped or an error field added
                    newRecord.put(rule.getTargetField() != null ? rule.getTargetField() + "_ERROR" : "TRANSFORMATION_ERROR", e.getMessage());
                }
            }
            transformedRecords.add(newRecord);
        }
        return transformedRecords;
    }

    private void applyRule(Map<String, Object> originalRecord, Map<String, Object> newRecord, TransformationRule rule) {
        String targetField = rule.getTargetField();
        if (targetField == null || targetField.trim().isEmpty()) {
            logger.warn("Rule found with no target field. Skipping rule: {}", rule);
            return;
        }

        Object sourceValue = null;
        if (rule.getSourceField() != null && originalRecord.containsKey(rule.getSourceField())) {
            sourceValue = originalRecord.get(rule.getSourceField());
        } else if (rule.getSourceField() != null && rule.getTransformationType() != TransformationRule.TransformationType.CONSTANT) {
             // Only log missing source field if it's not a CONSTANT type or if sourceField was actually specified
            logger.debug("Source field '{}' not found in record for rule type {}. Applying null or default if applicable.", rule.getSourceField(), rule.getTransformationType());
        }


        switch (rule.getTransformationType()) {
            case MAP:
                newRecord.put(targetField, sourceValue);
                break;
            case CONSTANT:
                newRecord.put(targetField, rule.getConstantValue());
                break;
            case CONVERT_TO_STRING:
                newRecord.put(targetField, sourceValue != null ? String.valueOf(sourceValue) : null);
                break;
            case CONVERT_TO_INTEGER:
                if (sourceValue != null) {
                    try {
                        if (sourceValue instanceof Number) {
                            newRecord.put(targetField, ((Number) sourceValue).intValue());
                        } else {
                            newRecord.put(targetField, Integer.parseInt(String.valueOf(sourceValue).trim()));
                        }
                    } catch (NumberFormatException e) {
                        logger.warn("Could not parse '{}' to Integer for target field '{}'. Setting null. Original: {}", sourceValue, targetField, originalRecord.get(rule.getSourceField()), e);
                        newRecord.put(targetField, null); // Or handle as error
                    }
                } else {
                    newRecord.put(targetField, null);
                }
                break;
            case CONVERT_TO_LONG:
                 if (sourceValue != null) {
                    try {
                        if (sourceValue instanceof Number) {
                            newRecord.put(targetField, ((Number) sourceValue).longValue());
                        } else {
                            newRecord.put(targetField, Long.parseLong(String.valueOf(sourceValue).trim()));
                        }
                    } catch (NumberFormatException e) {
                        logger.warn("Could not parse '{}' to Long for target field '{}'. Setting null. Original: {}", sourceValue, targetField, originalRecord.get(rule.getSourceField()), e);
                        newRecord.put(targetField, null);
                    }
                } else {
                    newRecord.put(targetField, null);
                }
                break;
            case CONVERT_TO_DOUBLE:
                if (sourceValue != null) {
                    try {
                        if (sourceValue instanceof Number) {
                            newRecord.put(targetField, ((Number) sourceValue).doubleValue());
                        } else {
                            // Handle potential commas in numbers if they are strings, e.g., "1,234.56"
                            String sVal = String.valueOf(sourceValue).trim().replace(",", "");
                            newRecord.put(targetField, Double.parseDouble(sVal));
                        }
                    } catch (NumberFormatException e) {
                        logger.warn("Could not parse '{}' to Double for target field '{}'. Setting null. Original: {}", sourceValue, targetField, originalRecord.get(rule.getSourceField()), e);
                        newRecord.put(targetField, null);
                    }
                } else {
                    newRecord.put(targetField, null);
                }
                break;
            case CONVERT_TO_BOOLEAN:
                 if (sourceValue != null) {
                    String sVal = String.valueOf(sourceValue).trim().toLowerCase();
                    if (sVal.equals("true") || sVal.equals("1") || sVal.equals("yes") || sVal.equals("y")) {
                        newRecord.put(targetField, true);
                    } else if (sVal.equals("false") || sVal.equals("0") || sVal.equals("no") || sVal.equals("n")) {
                        newRecord.put(targetField, false);
                    } else {
                        logger.warn("Could not parse '{}' to Boolean for target field '{}'. Setting null.", sourceValue, targetField);
                        newRecord.put(targetField, null); // Or based on strictness, throw error
                    }
                } else {
                    newRecord.put(targetField, null);
                }
                break;
            case CONVERT_TO_DATE:
                if (sourceValue != null) {
                    String dateFormat = rule.getParameters() != null ? rule.getParameters().get("dateFormatFrom") : "yyyy-MM-dd HH:mm:ss"; // Default format
                    if (dateFormat == null) {
                        logger.warn("dateFormatFrom parameter missing for CONVERT_TO_DATE rule for target field '{}'. Using default.", targetField);
                        dateFormat = "yyyy-MM-dd HH:mm:ss";
                    }
                    try {
                        // If sourceValue is already a Date or Long (timestamp), handle it
                        if (sourceValue instanceof java.util.Date) {
                             newRecord.put(targetField, sourceValue);
                        } else if (sourceValue instanceof Long) {
                             newRecord.put(targetField, new java.util.Date((Long)sourceValue));
                        } else {
                            SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
                            sdf.setLenient(false); // Strict parsing
                            newRecord.put(targetField, sdf.parse(String.valueOf(sourceValue).trim()));
                        }
                    } catch (ParseException e) {
                        logger.warn("Could not parse date '{}' with format '{}' for target field '{}'. Setting null. Original: {}", sourceValue, dateFormat, targetField, originalRecord.get(rule.getSourceField()), e);
                        newRecord.put(targetField, null);
                    }
                } else {
                    newRecord.put(targetField, null);
                }
                break;
            case CONCATENATE_FIELDS:
                StringBuilder concatenatedValue = new StringBuilder();
                String separator = rule.getParameters() != null ? rule.getParameters().get("concatenationSeparator") : "";
                if (rule.getSourceFields() != null && rule.getSourceFields().length > 0) {
                    for (int i = 0; i < rule.getSourceFields().length; i++) {
                        String fieldName = rule.getSourceFields()[i];
                        Object val = originalRecord.get(fieldName);
                        if (val != null) {
                            concatenatedValue.append(val);
                        }
                        if (i < rule.getSourceFields().length - 1) {
                            concatenatedValue.append(separator);
                        }
                    }
                    newRecord.put(targetField, concatenatedValue.toString());
                } else {
                     logger.warn("No sourceFields provided for CONCATENATE_FIELDS rule for target field '{}'.", targetField);
                     newRecord.put(targetField, null); // Or empty string
                }
                break;
            // Implement other types like SPLIT_FIELD, CUSTOM_SCRIPT later
            default:
                logger.warn("Unsupported transformation type: {} for target field '{}'", rule.getTransformationType(), targetField);
                newRecord.put(targetField, sourceValue); // Default to MAP behavior for unknown types for now
                break;
        }
    }
}
