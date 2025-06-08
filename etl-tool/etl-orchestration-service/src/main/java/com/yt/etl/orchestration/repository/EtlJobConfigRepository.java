package com.yt.etl.orchestration.repository;

import com.yt.etl.orchestration.entity.EtlJobConfigEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface EtlJobConfigRepository extends JpaRepository<EtlJobConfigEntity, String> {
    Optional<EtlJobConfigEntity> findByJobName(String jobName);
}
