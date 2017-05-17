package com.ge.current.em.custom.analytics.drools;

import com.ge.current.em.entities.analytics.RulesBaseFact;
import org.kie.api.runtime.StatelessKieSession;
import org.kie.internal.command.CommandFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by 212582112 on 10/3/16.
 */
public class IERulesUtil {
	private static StatelessKieSession kieSession = null;
	private static final Logger LOG = LoggerFactory.getLogger(IERulesUtil.class);

	public static synchronized void initRulesEngine(List<String> drls) {

		try {
			kieSession = KieSessionFactory.getSession(drls);
		} catch(Exception e ) {
			LOG.error("Error initializing session", e);
		}
	}

	public static void applyRule(RulesBaseFact rulesBaseFact) {
		if(kieSession == null) {
			throw new RuntimeException("KieSession not initialized.");
		}
		TrackingAgendaEventListener agendaEventListener = new TrackingAgendaEventListener();
		kieSession.addEventListener(agendaEventListener);
		kieSession.execute(CommandFactory.newInsert(rulesBaseFact));
		LOG.info("Listener: {}", agendaEventListener.ruleMatchedToString());
	}

	public static void resetKieSession() {
		KieSessionFactory.resetKieSession();
	}
}
