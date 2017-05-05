/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark.streaming;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.springframework.beans.factory.Aware;

public interface JavaStreamingContextAware extends Aware {
    void setJavaStreamingContext(JavaStreamingContext javaStreamingContext);
}
