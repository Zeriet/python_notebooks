package com.ge.current.em.custom.analytics.drools;

import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.Message;
import org.kie.api.builder.Results;
import org.kie.api.io.Resource;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.StatelessKieSession;
import org.kie.internal.io.ResourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ObjectInput;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by 212582112 on 9/29/16.
 * Updated by 212554696 on 01/04/17.
 */
public class KieSessionFactory {

	static final Logger LOG = LoggerFactory.getLogger(KieSessionFactory.class);

	private static volatile StatelessKieSession kieSession = null;
	private static final Object lock = new Object();

	public static synchronized StatelessKieSession getSession(List<String> drlURI) {

			if (kieSession == null) {
				synchronized(lock) {
					if (kieSession == null) {
						LOG.warn("trying to get KieSession");
						kieSession = initKieSession(drlURI);
					}
				}
			} else {
				LOG.warn("Kie Session was already initialized. Returning current Kie Session.");
			}
		return kieSession;
	}

	private static StatelessKieSession initKieSession(List<String> drls) {
		long startTime = System.currentTimeMillis();
		StatelessKieSession kieSessiontemp = null;
		LOG.info("Initializing Kie Session at " + startTime);

		KieServices kieServices = KieServices.Factory.get();
		KieFileSystem kieFileSystem = kieServices.newKieFileSystem();
		int appender = 0;
		Map<String, String> drlsMap = new HashMap<>();
		for(String drl: drls) {
			drlsMap.put(drl,drl);
		}
		LOG.info("Initializing Kie Session at the drls size: {}" , drls.size());
		LOG.info("Initializing Kie Session at the drlsMap size: {}" , drlsMap.size());
		for(String drl: drlsMap.keySet()) {
			if(drl != null) {
//				LOG.info(drl);
				Resource resource = ResourceFactory.newReaderResource(new StringReader(drl));
				// TODO: need verification. source path is needed in the method but file is not necessary since we have the resource.
				kieFileSystem.write("src/main/resources/drl/rule" + appender + ".drl", resource);
				appender++;
			}
		}
		KieBuilder kieBuilder = kieServices.newKieBuilder( kieFileSystem).buildAll();
		Results results = kieBuilder.getResults();
		if( results.hasMessages( Message.Level.ERROR ) ){
			LOG.error("Error while initializing session: {}", results.getMessages());
			throw new IllegalStateException( "Error while initializing session: " + results.getMessages());
		}
		KieContainer kContainer = kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId());
		KieBase kieBase = kContainer.getKieBase();
		kieSessiontemp = kieBase.newStatelessKieSession();

		long endTime = System.currentTimeMillis();


		LOG.info("Kie session has been initialized at " + endTime);
		LOG.info("Kie session initialization took " + (endTime - startTime) + " ms.");
		return kieSessiontemp;
	}

	public static void resetKieSession() {
		kieSession = null;
	}
}
