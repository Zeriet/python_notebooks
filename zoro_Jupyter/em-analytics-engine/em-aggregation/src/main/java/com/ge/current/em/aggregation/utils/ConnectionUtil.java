package com.ge.current.em.aggregation.utils;

import com.ge.current.ie.bean.PropertiesBean;
import com.ge.current.ie.bean.SparkConfigurations;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import static com.ge.current.ie.bean.SparkConfigurations.CASSANDRA_CONNECTION_PORT;

/**
 * Created by 212582112 on 1/31/17.
 */
public class ConnectionUtil {
    private static final Logger LOGGER = Logger.getLogger(ConnectionUtil.class);

    /**
     * Adds Cassandra connection information to the spark context
     * to read and write data.
     *
     * @param sparkConf
     * @param propertiesBean
     */
    public static SparkConf configureCassandraConnection(SparkConf sparkConf,
                                                         PropertiesBean propertiesBean) {

        if(propertiesBean.getValue(SparkConfigurations.CASSANDRA_USERNAME) == null || !propertiesBean.getValue(SparkConfigurations.CASSANDRA_USERNAME).isEmpty()) {
            sparkConf.set("spark.cassandra.auth.username", propertiesBean.getValue(SparkConfigurations.CASSANDRA_USERNAME));
            LOGGER.debug("Cassandra Uname: " + sparkConf.get("spark.cassandra.auth.username"));
        }

        if(propertiesBean.getValue(SparkConfigurations.CASSANDRA_PASSWORD) == null || !propertiesBean.getValue(SparkConfigurations.CASSANDRA_PASSWORD).isEmpty()) {
            sparkConf.set("spark.cassandra.auth.password", propertiesBean.getValue(SparkConfigurations.CASSANDRA_PASSWORD));
            LOGGER.debug("Cassandra Passwd: " + sparkConf.get("spark.cassandra.auth.password"));
        }

        sparkConf.set("spark.cassandra.connection.host", propertiesBean.getValue(SparkConfigurations.CASSANDRA_CONNECTION_HOST));
        sparkConf.set("spark.cassandra.connection.port", propertiesBean.getValue(CASSANDRA_CONNECTION_PORT));

        LOGGER.debug("Cassandra Hosts: " + sparkConf.get("spark.cassandra.connection.host"));
        LOGGER.debug("Cassandra Port: " + sparkConf.get("spark.cassandra.connection.port"));

        return sparkConf;
    }
}