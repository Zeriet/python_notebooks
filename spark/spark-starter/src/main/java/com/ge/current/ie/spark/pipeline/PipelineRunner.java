/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.pipeline;

import java.util.List;
import java.util.function.Supplier;

import org.springframework.boot.CommandLineRunner;

public class PipelineRunner implements CommandLineRunner {
    private final PipelineRegistry registry;

    public PipelineRunner(PipelineRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void run(String... args) throws Exception {
        registry.getPipelines()
                .forEach((s, p) -> runAllPipelines(s, p));
    }

    private void runAllPipelines(Supplier supplier, List<Pipeline> pipelines) {
        pipelines.forEach(p -> runSinglePipe(supplier, p));
    }

    private void runSinglePipe(Supplier supplier, Pipeline pipeline) {
        Object data = supplier.get();
        pipeline.flow(data);
    }
}
