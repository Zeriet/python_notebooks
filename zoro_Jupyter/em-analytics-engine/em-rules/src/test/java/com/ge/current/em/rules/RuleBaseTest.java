package com.ge.current.em.rules;

import com.ge.current.em.custom.analytics.drools.IERulesUtil;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by 212554696 on 12/22/16.
 */
public class RuleBaseTest {
	private static final Logger LOG = LoggerFactory.getLogger(RuleBaseTest.class);

	public static final long ONE_MINUTE_IN_MILLIS=60000;

	public ClassLoader classloader;

	public List<String> rules;

	public void init() {
		BasicConfigurator.configure();
		classloader = Thread.currentThread().getContextClassLoader();
	}

	public void initRules(String ... rulesDrls) {
		BasicConfigurator.configure();
		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
		rules = new LinkedList<>();
		try {
			InputStream inputStream = null;
			for(String path : rulesDrls) {
				inputStream = classloader.getResourceAsStream("drl/" + path);
				String fileInString = IOUtils.toString(inputStream, "UTF-8");
				rules.add(fileInString);
			}
			inputStream.close();
		} catch (IOException eIO) {
			LOG.error("Error loading file: {}", eIO);
		}
		IERulesUtil.initRulesEngine(rules);
		rules = new LinkedList<>();
	}

	public void resetKieSession() {
		IERulesUtil.resetKieSession();
	}
}
