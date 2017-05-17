package com.ge.current.em.custom.analytics.batch.main;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.env.SimpleCommandLinePropertySource;

import com.ge.current.em.custom.analytics.batch.ISparkRule;

@Configuration
@ComponentScan(basePackages = "com.ge.current")
public class SparkCustomAnalyticsMain {
	  private static final Logger LOG = LoggerFactory.getLogger(SparkCustomAnalyticsMain.class);



	    /**
	     * Main entrypoint into the application. Loads the environment variables and
	     * @param args Command-line arguments.
	     * @throws IOException 
	     */
	    public static void main(String[] args) throws IOException {
	        System.out.println("In SparkCustomAnalyticsMain method");

	        if (args.length < 8) {
	            LOG.error("Incorrect usage, must provide active profile, spark stream class to run, ssl usage, protocol ");
	            System.exit(-1);
	        }
	        String sparkStreamClass = args[1];
	        String resrcType = args[2];
	        String tableName = args[3];
	        String groupBy = args[4];
	        String ruleNames = args[5];
	        String timeFrame = args[6];
	        String timeZonesList = args[7];
	        String startTime = args[8];
	        String endTime = args[9];

	        LOG.info("args: {}, {}" + args[0] + ", " + args[1] + ", " + args[2] +","+ args[3] + ", " + args[4],  ", " + args[5],  ", " + args[6],  ", " + args[7],  ", " + args[8] + ", " + args[9]);

	        AnnotationConfigApplicationContext applicationContext = new AnnotationConfigApplicationContext();
	        applicationContext.register(SparkCustomAnalyticsMain.class);
	        applicationContext.getEnvironment().getPropertySources().addFirst(new SimpleCommandLinePropertySource(args));
	        applicationContext.refresh();
	        applicationContext.start();

	        Environment env = applicationContext.getEnvironment();

	        ISparkRule sparkRule = null;
	        try {
	        	sparkRule = (ISparkRule)applicationContext.getBean(Class.forName(sparkStreamClass));
	        } catch (ClassNotFoundException cnfe) {
	            LOG.error("Could not find class: " + sparkStreamClass);
	            System.exit(-1);
	        } catch (BeanCreationException bce) {
	            LOG.error("Could not create bean of type: " + sparkStreamClass);
	            System.exit(-1);
	        }
	        sparkRule.setRescrType(resrcType);
	        sparkRule.setTableName(tableName);
	        sparkRule.setGroupBy(groupBy);
	        sparkRule.setTimeZone(timeZonesList);
	        sparkRule.setRuleNamesAsString(ruleNames.replaceAll("_", " "));
	        sparkRule.setTimeFrame(timeFrame);
	        sparkRule.setAppName(ruleNames);
	        sparkRule.run(env,startTime,endTime,timeZonesList);

	    }
}
