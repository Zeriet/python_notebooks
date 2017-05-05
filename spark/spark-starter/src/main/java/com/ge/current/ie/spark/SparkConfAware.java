/*
 * Copyright (c) 2016 GE. All Rights Reserved.
 * GE Confidential: Restricted Internal Distribution
 */
package com.ge.current.ie.spark;

import org.apache.spark.SparkConf;
import org.springframework.beans.factory.Aware;

public interface SparkConfAware extends Aware {
    void setSparkConf(SparkConf sparkConf);
}
