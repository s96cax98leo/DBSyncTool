package com.yt.etl.common.model;

import java.util.List;
import java.util.Objects;

public class JobTransformationConfig {

    private List<TransformationRule> rules;

    public JobTransformationConfig() {
    }

    public JobTransformationConfig(List<TransformationRule> rules) {
        this.rules = rules;
    }

    public List<TransformationRule> getRules() {
        return rules;
    }

    public void setRules(List<TransformationRule> rules) {
        this.rules = rules;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobTransformationConfig that = (JobTransformationConfig) o;
        return Objects.equals(rules, that.rules);
    }

    @Override
    public int hashCode() {
        return Objects.hash(rules);
    }

    @Override
    public String toString() {
        return "JobTransformationConfig{" +
                "rules=" + rules +
                '}';
    }
}
