package com.yt.etl.orchestration.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yt.etl.common.model.DataRecordBatch;
import com.yt.etl.common.model.DatabaseConnectionConfig;
import com.yt.etl.common.model.EtlJobConfig; // Using common model for now
import com.yt.etl.common.model.JobTransformationConfig;
import com.yt.etl.orchestration.dto.CreateJobRequest;
import com.yt.etl.orchestration.dto.JobConfigResponse;
import com.yt.etl.orchestration.dto.StartJobResponse;
import com.yt.etl.orchestration.entity.DatabaseConnectionConfigEmbeddable;
import com.yt.etl.orchestration.entity.EtlJobConfigEntity;
import com.yt.etl.orchestration.entity.JobExecutionLogEntity;
import com.yt.etl.orchestration.entity.JobExecutionTableLogEntity; // Import new entity
import com.yt.etl.orchestration.repository.EtlJobConfigRepository;
import com.yt.etl.orchestration.repository.JobExecutionLogRepository;
import com.yt.etl.orchestration.repository.JobExecutionTableLogRepository; // Import new repository

import jakarta.validation.Valid;
import org.springframework.transaction.annotation.Transactional; // For transactional method
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/jobs")
public class EtlJobController {

    private static final Logger logger = LoggerFactory.getLogger(EtlJobController.class);
    private final ObjectMapper objectMapper = new ObjectMapper(); // For converting Map<String, String> to JSON String for Embeddable

    private final EtlJobConfigRepository jobConfigRepository;
    private final JobExecutionLogRepository jobExecutionLogRepository;
    private final JobExecutionTableLogRepository jobExecutionTableLogRepository; // Inject new repository
    private final KafkaTemplate<String, DataRecordBatch> kafkaTemplate;

    @Value("${etl.topics.command.extract}")
    private String extractionCommandsTopic;

    @Autowired
    public EtlJobController(EtlJobConfigRepository jobConfigRepository,
                            JobExecutionLogRepository jobExecutionLogRepository,
                            JobExecutionTableLogRepository jobExecutionTableLogRepository, // Add to constructor
                            KafkaTemplate<String, DataRecordBatch> kafkaTemplate) {
        this.jobConfigRepository = jobConfigRepository;
        this.jobExecutionLogRepository = jobExecutionLogRepository;
        this.jobExecutionTableLogRepository = jobExecutionTableLogRepository; // Initialize
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping
    public ResponseEntity<JobConfigResponse> createJob(@Valid @RequestBody CreateJobRequest createRequest) {
        if (jobConfigRepository.findByJobName(createRequest.getJobName()).isPresent()) {
             throw new ResponseStatusException(HttpStatus.CONFLICT, "Job with name '" + createRequest.getJobName() + "' already exists.");
        }
        EtlJobConfigEntity entity = mapToEntity(createRequest);
        entity.setJobId(UUID.randomUUID().toString()); // Generate Job ID
        EtlJobConfigEntity savedEntity = jobConfigRepository.save(entity);
        logger.info("Created ETL Job with ID: {} and Name: {}", savedEntity.getJobId(), savedEntity.getJobName());
        return ResponseEntity.status(HttpStatus.CREATED).body(mapToResponse(savedEntity));
    }

    @GetMapping("/{jobId}")
    public ResponseEntity<JobConfigResponse> getJobById(@PathVariable String jobId) {
        return jobConfigRepository.findById(jobId)
                .map(this::mapToResponse)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<JobConfigResponse>> getAllJobs() {
        List<JobConfigResponse> jobResponses = jobConfigRepository.findAll().stream()
                .map(this::mapToResponse)
                .collect(Collectors.toList());
        return ResponseEntity.ok(jobResponses);
    }

    @PostMapping("/{jobId}/start")
    @Transactional // Make this transactional to ensure all initial logs are saved or none
    public ResponseEntity<StartJobResponse> startJob(@PathVariable String jobId) {
        EtlJobConfigEntity jobConfigEntity = jobConfigRepository.findById(jobId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Job config with ID " + jobId + " not found."));

        // Create main job execution log
        JobExecutionLogEntity jobExecutionLog = new JobExecutionLogEntity();
        jobExecutionLog.setJobConfigId(jobConfigEntity.getJobId());
        // jobExecutionLog.setStartTime(LocalDateTime.now()); // Handled by @PrePersist or constructor
        jobExecutionLog.setStatus("SUBMITTED");
        jobExecutionLog.setLastMessage("Job submitted. Initializing table processing logs.");
        int totalTables = jobConfigEntity.getTablesToProcess() != null ? jobConfigEntity.getTablesToProcess().size() : 0;
        jobExecutionLog.setTotalTablesToProcess(totalTables);
        jobExecutionLog.setTablesCompleted(0);
        jobExecutionLog.setTablesFailed(0);
        // Initialize aggregate record counts
        jobExecutionLog.setTotalRecordsExtracted(0L);
        jobExecutionLog.setTotalRecordsTransformed(0L);
        jobExecutionLog.setTotalRecordsLoaded(0L);

        JobExecutionLogEntity savedLog = jobExecutionLogRepository.save(jobExecutionLog);
        logger.info("Job ID: {} - Execution ID: {} - Status: SUBMITTED. Total tables: {}",
                    jobId, savedLog.getExecutionId(), totalTables);

        // Create table-specific logs and send commands
        for (String tableName : jobConfigEntity.getTablesToProcess()) {
            // Create log for each table
            JobExecutionTableLogEntity tableLog = new JobExecutionTableLogEntity(savedLog.getExecutionId(), tableName, "PENDING");
            tableLog.setStartTime(LocalDateTime.now()); // Explicitly set start time for table log
            jobExecutionTableLogRepository.save(tableLog);
            logger.debug("Created PENDING log for table {} in execution {}", tableName, savedLog.getExecutionId());

            JobTransformationConfig transformationConfig = jobConfigEntity.getTableTransformationConfigs() != null ?
                    jobConfigEntity.getTableTransformationConfigs().get(tableName) : null;

            // The DataRecordBatch acts as the command message for the extractor
            DataRecordBatch extractCommand = new DataRecordBatch();
            extractCommand.setJobId(jobConfigEntity.getJobId());
            extractCommand.setBatchId(UUID.randomUUID().toString()); // Unique ID for this command/initial batch
            extractCommand.setTableId(tableName);
            // Assuming target table name is same as source if not changed by transformation.
            // This might be better determined by transform service or explicitly set in job config.
            extractCommand.setTargetTableId(tableName);
            extractCommand.setJobTransformationConfig(transformationConfig);
            extractCommand.setRecords(null); // No records in the command message itself
            extractCommand.setLastBatch(false); // This is a command to start, not a data batch itself.
                                                // The extractor will set this for the actual data.
            extractCommand.setSequenceNumber(0); // Initial command
            extractCommand.setExecutionId(savedLog.getExecutionId().toString());
            extractCommand.setSourceDbConfig(mapEmbeddableToDbConfig(jobConfigEntity.getSourceDbConfig()));
            extractCommand.setTargetDbConfig(mapEmbeddableToDbConfig(jobConfigEntity.getTargetDbConfig()));
            // extractCommand.setJobTransformationConfig(transformationConfig); // This was already set above

            try {
                kafkaTemplate.send(extractionCommandsTopic, jobConfigEntity.getJobId(), extractCommand);
                logger.info("Sent extraction command for Job ID: {}, Execution ID: {}, Table: {} to topic {}",
                        jobConfigEntity.getJobId(), savedLog.getExecutionId(), tableName, extractionCommandsTopic);
            } catch (Exception e) {
                logger.error("Failed to send extraction command for Job ID: {}, Table: {}. Error: {}",
                        jobConfigEntity.getJobId(), tableName, e.getMessage(), e);
                // Update job execution log to FAILED if kafka send fails critically
                savedLog.setStatus("FAILED_TO_START");
                savedLog.setLastMessage("Failed to send command to Kafka for table " + tableName + ": " + e.getMessage());
                savedLog.setEndTime(LocalDateTime.now());
                jobExecutionLogRepository.save(savedLog);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                                     .body(new StartJobResponse(jobId, savedLog.getExecutionId(), "Failed to send command for table " + tableName, "FAILED_TO_START"));
            }
        }

        savedLog.setStatus("PROCESSING_INITIATED"); // Main job log status
        savedLog.setLastMessage("All table processing commands sent to Kafka. Current tables pending: " + totalTables);
        jobExecutionLogRepository.save(savedLog);

        return ResponseEntity.ok(new StartJobResponse(jobId, savedLog.getExecutionId(),
                                 "Job processing initiated for " + totalTables + " table(s).", "PROCESSING_INITIATED"));
    }


    // --- Helper methods for mapping ---
    private DatabaseConnectionConfigEmbeddable mapDbConfigToEmbeddable(DatabaseConnectionConfig commonConfig) {
        if (commonConfig == null) return null;
        DatabaseConnectionConfigEmbeddable embeddable = new DatabaseConnectionConfigEmbeddable();
        embeddable.setConnectionName(commonConfig.getConnectionName());
        embeddable.setJdbcUrl(commonConfig.getJdbcUrl());
        embeddable.setUsername(commonConfig.getUsername());
        embeddable.setPassword(commonConfig.getPassword()); // Consider encryption before setting
        embeddable.setDriverClassName(commonConfig.getDriverClassName());
        if (commonConfig.getAdditionalProperties() != null && !commonConfig.getAdditionalProperties().isEmpty()) {
            try {
                embeddable.setAdditionalPropertiesJson(objectMapper.writeValueAsString(commonConfig.getAdditionalProperties()));
            } catch (JsonProcessingException e) {
                logger.error("Error serializing additional DB properties to JSON: {}", e.getMessage(), e);
                // Decide on error handling: throw, or set to null/empty
            }
        }
        return embeddable;
    }

    private DatabaseConnectionConfig mapEmbeddableToDbConfig(DatabaseConnectionConfigEmbeddable embeddable) {
        if (embeddable == null) return null;
        DatabaseConnectionConfig commonConfig = new DatabaseConnectionConfig();
        commonConfig.setConnectionName(embeddable.getConnectionName());
        commonConfig.setJdbcUrl(embeddable.getJdbcUrl());
        commonConfig.setUsername(embeddable.getUsername());
        commonConfig.setPassword(embeddable.getPassword()); // Password will be retrieved (potentially decrypted)
        commonConfig.setDriverClassName(embeddable.getDriverClassName());
        if (embeddable.getAdditionalPropertiesJson() != null && !embeddable.getAdditionalPropertiesJson().isEmpty()) {
            try {
                commonConfig.setAdditionalProperties(objectMapper.readValue(embeddable.getAdditionalPropertiesJson(), new TypeReference<Map<String, String>>() {}));
            } catch (JsonProcessingException e) {
                logger.error("Error deserializing additional DB properties from JSON: {}", e.getMessage(), e);
            }
        }
        return commonConfig;
    }

    private EtlJobConfigEntity mapToEntity(CreateJobRequest dto) {
        EtlJobConfigEntity entity = new EtlJobConfigEntity();
        // JobId is set by the service
        entity.setJobName(dto.getJobName());
        entity.setSourceDbConfig(mapDbConfigToEmbeddable(dto.getSourceDbConfig()));
        entity.setTargetDbConfig(mapDbConfigToEmbeddable(dto.getTargetDbConfig()));
        entity.setTablesToProcess(dto.getTablesToProcess());
        entity.setTableTransformationConfigs(dto.getTableTransformationConfigs());
        return entity;
    }

    private JobConfigResponse mapToResponse(EtlJobConfigEntity entity) {
        JobConfigResponse response = new JobConfigResponse();
        response.setJobId(entity.getJobId());
        response.setJobName(entity.getJobName());
        response.setSourceDbConfig(mapEmbeddableToDbConfig(entity.getSourceDbConfig()));
        response.setTargetDbConfig(mapEmbeddableToDbConfig(entity.getTargetDbConfig()));
        response.setTablesToProcess(entity.getTablesToProcess());
        response.setTableTransformationConfigs(entity.getTableTransformationConfigs());
        return response;
    }
}
