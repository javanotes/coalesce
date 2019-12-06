package io.reactiveminds.datagrid.rules;

import java.io.File;
import java.io.FileReader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.PostConstruct;

import org.apache.avro.specific.SpecificRecord;
import org.jeasy.rules.api.Rules;
import org.jeasy.rules.api.RulesEngine;
import org.jeasy.rules.core.DefaultRulesEngine;
import org.jeasy.rules.mvel.MVELRuleFactory;
import org.jeasy.rules.support.JsonRuleDefinitionReader;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

class EasyRulesEngine implements RuleEngine {

	private final ConcurrentMap<String, Rules> rules = new ConcurrentHashMap<String, Rules>();
	private MVELRuleFactory ruleFactory;
	private RulesEngine engine;
	
	@PostConstruct
	void init() {
		ruleFactory = new MVELRuleFactory(new JsonRuleDefinitionReader());
		engine = new DefaultRulesEngine();
	}
	@Override
	public String register(String ruleFile)
			throws Exception {
		File f = ResourceUtils.getFile(ruleFile);
		Rules ruleset = ruleFactory.createRules(new FileReader(f));
		String name = StringUtils.stripFilenameExtension(f.getName());
		if(rules.putIfAbsent(name, ruleset) != null) {
			throw new Exception("duplicate ruleset: "+name);
		}
		return name;
	}

	@Override
	public <T extends SpecificRecord> void execute(String ruleSet, Facts facts) throws Exception {
		if(rules.containsKey(ruleSet)) {
			Rules r = rules.get(ruleSet);
			org.jeasy.rules.api.Facts fact = new org.jeasy.rules.api.Facts();
			fact.put(VAR_INCOMING, facts.getRequest());
			fact.put(VAR_ONFILE, facts.getOnfile());
			
			engine.fire(r, fact);
		}
		throw new Exception("ruleset not registered: "+ruleSet);
	}

}
