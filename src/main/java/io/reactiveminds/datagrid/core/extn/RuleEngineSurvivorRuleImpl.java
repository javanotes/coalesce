package io.reactiveminds.datagrid.core.extn;

import javax.annotation.PostConstruct;

import org.apache.avro.specific.SpecificRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import io.reactiveminds.datagrid.api.GridContext;
import io.reactiveminds.datagrid.err.ConfigurationException;
import io.reactiveminds.datagrid.err.ProcessFailedException;
import io.reactiveminds.datagrid.rules.Facts;
import io.reactiveminds.datagrid.rules.RuleEngine;

public class RuleEngineSurvivorRuleImpl extends SpecificSurvivorRule {

	@Autowired
	RuleEngine engine;
	
	@Value("${coalesce.rules.ruleset}")
	private String rulesetPath;
	private String rulesetName;
	
	@PostConstruct
	void init() {
		try {
			rulesetName = engine.register(rulesetPath);
		} catch (Exception e) {
			throw new ConfigurationException("Unable to load ruleset", e);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	protected <V extends SpecificRecord> V doMerge(V onfile, V request, GridContext grid) {
		Facts fact = new Facts();
		fact.setOnfile(onfile);
		fact.setRequest(request);
		try {
			engine.execute(rulesetName, fact);
			return (V) fact.getOnfile();
			
		} catch (Exception e) {
			throw new ProcessFailedException("Error executing rule engine: "+e.getMessage(), e);
		}
	}

}
