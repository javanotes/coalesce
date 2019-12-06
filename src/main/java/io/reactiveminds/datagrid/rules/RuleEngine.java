package io.reactiveminds.datagrid.rules;

import org.apache.avro.specific.SpecificRecord;

public interface RuleEngine {

	String VAR_INCOMING = "__r";
	String VAR_ONFILE = "__c";
	String register(String ruleFile)
			throws Exception;

	<T extends SpecificRecord> void execute(String ruleSet, Facts facts) throws Exception;

}