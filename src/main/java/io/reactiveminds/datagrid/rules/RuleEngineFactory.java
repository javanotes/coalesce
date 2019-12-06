package io.reactiveminds.datagrid.rules;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;

public class RuleEngineFactory implements FactoryBean<RuleEngine> {

	@Value("${coalesce.rules.provider:}")
	private String provider;
	@Override
	public RuleEngine getObject() throws Exception {
		return provider.equalsIgnoreCase("drools") ? new DroolsEngine() : new EasyRulesEngine();
	}

	@Override
	public Class<?> getObjectType() {
		return RuleEngine.class;
	}

}
