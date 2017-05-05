/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.config;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import com.ge.current.ie.spark.pipeline.PipelineRegistry;
import com.ge.current.ie.spark.pipeline.PipelineRunner;

@Configuration
public class DelegatingPipelineConfiguration {
    private final List<PipelineConfigurer> configurers = new ArrayList<>();

    @Autowired(required = false)
    private void setConfigurers(List<PipelineConfigurer> configurers) {
        this.configurers.addAll(configurers);
    }

    @Bean
    public PipelineRegistry pipelineRegistry() {
        PipelineRegistry registry = new PipelineRegistry();
        configurers.forEach(c -> c.registerPipelines(registry));
        return registry;
    }

    @Bean
    @Order(value = Ordered.LOWEST_PRECEDENCE)
    public PipelineRunner pipelineRunner(PipelineRegistry registry, JavaSparkContext javaSparkContext) {
        return new PipelineRunner(registry);
    }
}
