/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.config;

import com.ge.current.ie.spark.pipeline.PipelineRegistry;

public interface PipelineConfigurer {
    void registerPipelines(PipelineRegistry registry);
}
