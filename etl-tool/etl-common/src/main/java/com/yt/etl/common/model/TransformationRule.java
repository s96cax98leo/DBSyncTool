package com.yt.etl.common.model;

import java.util.Map;
import java.util.Objects;

public class TransformationRule {

    public enum TransformationType {
        MAP,                // Simple field-to-field mapping
        CONVERT_TO_STRING,  // Convert to String
        CONVERT_TO_INTEGER, // Convert to Integer
        CONVERT_TO_LONG,    // Convert to Long
        CONVERT_TO_DOUBLE,  // Convert to Double
        CONVERT_TO_BOOLEAN, // Convert to Boolean
        CONVERT_TO_DATE,    // Convert to Date (java.util.Date or LocalDate)
        CONSTANT,           // Set a field to a constant value
        CONCATENATE_FIELDS, // Concatenate multiple source fields into one target field
        SPLIT_FIELD,        // Split a source field into multiple target fields (more complex, for later)
        CUSTOM_SCRIPT       // Execute a small script (e.g., Groovy, JavaScript) for complex logic (for later)
        // Potentially more: FORMAT_NUMBER, FORMAT_DATE, LOOKUP_VALUE, HASH_FIELD, MASK_FIELD etc.
    }

    private String sourceField; // Can be null for CONSTANT type
    private String[] sourceFields; // For CONCATENATE_FIELDS or other multi-field operations
    private String targetField;
    private TransformationType transformationType;
    private String constantValue; // Used if transformationType is CONSTANT
    private Map<String, String> parameters; // For date formats, concatenation separators, script content, etc.
                                            // e.g., "dateFormatFrom": "yyyyMMdd", "dateFormatTo": "dd/MM/yyyy"
                                            // e.g., "concatenationSeparator": "_"
                                            // e.g., "splitDelimiter": "," , "targetFieldNames": "fieldA,fieldB"


    public TransformationRule() {
    }

    public TransformationRule(String sourceField, String targetField, TransformationType transformationType) {
        this.sourceField = sourceField;
        this.targetField = targetField;
        this.transformationType = transformationType;
    }

    public TransformationRule(String targetField, TransformationType transformationType, String constantValue) {
        if (transformationType != TransformationType.CONSTANT) {
            throw new IllegalArgumentException("This constructor is for CONSTANT transformation type only.");
        }
        this.targetField = targetField;
        this.transformationType = transformationType;
        this.constantValue = constantValue;
    }


    // Getters and Setters
    public String getSourceField() {
        return sourceField;
    }

    public void setSourceField(String sourceField) {
        this.sourceField = sourceField;
    }

    public String[] getSourceFields() {
        return sourceFields;
    }

    public void setSourceFields(String[] sourceFields) {
        this.sourceFields = sourceFields;
    }

    public String getTargetField() {
        return targetField;
    }

    public void setTargetField(String targetField) {
        this.targetField = targetField;
    }

    public TransformationType getTransformationType() {
        return transformationType;
    }

    public void setTransformationType(TransformationType transformationType) {
        this.transformationType = transformationType;
    }

    public String getConstantValue() {
        return constantValue;
    }

    public void setConstantValue(String constantValue) {
        this.constantValue = constantValue;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TransformationRule that = (TransformationRule) o;
        return Objects.equals(sourceField, that.sourceField) &&
               java.util.Arrays.equals(sourceFields, that.sourceFields) &&
               Objects.equals(targetField, that.targetField) &&
               transformationType == that.transformationType &&
               Objects.equals(constantValue, that.constantValue) &&
               Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(sourceField, targetField, transformationType, constantValue, parameters);
        result = 31 * result + java.util.Arrays.hashCode(sourceFields);
        return result;
    }

    @Override
    public String toString() {
        return "TransformationRule{" +
                "sourceField='" + sourceField + '\'' +
                ", sourceFields=" + java.util.Arrays.toString(sourceFields) +
                ", targetField='" + targetField + '\'' +
                ", transformationType=" + transformationType +
                ", constantValue='" + constantValue + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}
