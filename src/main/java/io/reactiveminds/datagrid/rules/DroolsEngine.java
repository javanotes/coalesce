package io.reactiveminds.datagrid.rules;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import javax.annotation.PostConstruct;
import javax.rules.ConfigurationException;
import javax.rules.RuleRuntime;
import javax.rules.RuleServiceProvider;
import javax.rules.RuleServiceProviderManager;
import javax.rules.StatelessRuleSession;
import javax.rules.admin.LocalRuleExecutionSetProvider;
import javax.rules.admin.RuleAdministrator;
import javax.rules.admin.RuleExecutionSet;
import javax.rules.admin.RuleExecutionSetCreateException;
import javax.rules.admin.RuleExecutionSetRegisterException;

import org.apache.avro.specific.SpecificRecord;
import org.drools.jsr94.rules.RuleServiceProviderImpl;
import org.springframework.util.ResourceUtils;

class DroolsEngine implements RuleEngine {

	static final String PROVIDER = "http://drools.org/";

	private LocalRuleExecutionSetProvider ruleExecutionSetProvider;
	private RuleAdministrator ruleAdministrator;
	private RuleRuntime ruleRuntime;
	
	public static void main(String[] args) throws Exception{
		DroolsEngine engine = new DroolsEngine();
		engine.init();
		engine.register("classpath:rules.drl");
	}
	@PostConstruct
	void init() throws ClassNotFoundException, ConfigurationException, RuleExecutionSetCreateException, IOException,
			RuleExecutionSetRegisterException {
		
		Class.forName(RuleServiceProviderImpl.class.getName());

		// Get service provider
		RuleServiceProvider ruleServiceProvider = RuleServiceProviderManager.getRuleServiceProvider(PROVIDER);

		// Get the RuleAdministration
		ruleAdministrator = ruleServiceProvider.getRuleAdministrator();

		ruleExecutionSetProvider = ruleAdministrator.getLocalRuleExecutionSetProvider(null);
		ruleRuntime = ruleServiceProvider.getRuleRuntime();
	}
	
	@Override
	public String register(String ruleFile) throws RuleExecutionSetCreateException, IOException, RuleExecutionSetRegisterException {
		// Create the RuleExecutionSet for the drl
		RuleExecutionSet ruleExecutionSet =
				ruleExecutionSetProvider.createRuleExecutionSet(new FileReader( ResourceUtils.getFile(ruleFile)), null);

		// Register the RuleExecutionSet with the RuleAdministrator
		String uri = ruleExecutionSet.getName();
		ruleAdministrator.registerRuleExecutionSet(uri, ruleExecutionSet, null);
		return uri;
	}
	
	@Override
	public <T extends SpecificRecord> void execute(String ruleSet, Facts facts) throws Exception {
		StatelessRuleSession session = (StatelessRuleSession) ruleRuntime.createRuleSession( ruleSet, null, RuleRuntime.STATELESS_SESSION_TYPE );
		try {
			session.executeRules(Arrays.asList(facts));
		} finally {
			session.release();
		}
		
	}
}
