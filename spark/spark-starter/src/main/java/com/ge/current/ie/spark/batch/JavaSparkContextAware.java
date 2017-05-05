/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.batch;

import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.Aware;

public interface JavaSparkContextAware extends Aware {
    void setJavaSparkContext(JavaSparkContext javaSparkContext);
}
