package com.ge.current.em.util;

/**
 * Created by 212577826 on 5/2/17.
 */
public class SparkRulesConfig {

    public enum Configurations{

        UAA_TOKEN_URL("uaa.token.url"),
        UAA_CLIENT_ID("uaa.client.id"),
        UAA_CLIENT_SECRET("uaa.client.secret"),
        UAA_CLIENT_SCOPE("uaa.client.scope"),
        APP_PROXY_ENABLED("app.proxy.enabled");

        String configName;

        Configurations(String configName) {
            this.configName = configName;
        }

        public String getConfigName() {
            return configName;
        }
    }
}
