package com.yt.etl.orchestration.repository;

import com.yt.etl.orchestration.entity.JobExecutionLogEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface JobExecutionLogRepository extends JpaRepository<JobExecutionLogEntity, Long> {
    List<JobExecutionLogEntity> findByJobConfigIdOrderByStartTimeDesc(String jobConfigId);
    // Additional query methods can be added here as needed
    // For example, find by status, find by jobConfigId and status, etc.
}
