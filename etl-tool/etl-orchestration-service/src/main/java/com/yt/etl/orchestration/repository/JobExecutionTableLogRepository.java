package com.yt.etl.orchestration.repository;

import com.yt.etl.orchestration.entity.JobExecutionTableLogEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface JobExecutionTableLogRepository extends JpaRepository<JobExecutionTableLogEntity, Long> {

    List<JobExecutionTableLogEntity> findByExecutionId(Long executionId);

    Optional<JobExecutionTableLogEntity> findByExecutionIdAndTableName(Long executionId, String tableName);

    List<JobExecutionTableLogEntity> findByExecutionIdAndStatus(Long executionId, String status);
}
