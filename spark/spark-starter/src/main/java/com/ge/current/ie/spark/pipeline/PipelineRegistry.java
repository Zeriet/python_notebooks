/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.pipeline;

import java.util.function.Supplier;

import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

public class PipelineRegistry {
    private MultiValueMap<Supplier, Pipeline> supplierPipelineMap = new LinkedMultiValueMap<>();

    public void addHandler(Supplier supplier, Pipeline pipeline) {
        supplierPipelineMap.add(supplier, pipeline);
    }

    public MultiValueMap<Supplier, Pipeline> getPipelines() {
        return supplierPipelineMap;
    }
}
