package io.reactiveminds.datagrid.core;

import java.io.Flushable;

import io.reactiveminds.datagrid.api.FlushHandler;
import io.reactiveminds.datagrid.api.SurvivorRule;
import io.reactiveminds.datagrid.err.ProcessFailedException;
import io.reactiveminds.datagrid.vo.DataEvent;

public interface IProcessor extends Flushable{
	/**
	 * 
	 * @param key
	 * @param value
	 * @param valueSchema
	 * @param mapName
	 */
	void process(DataEvent request) throws ProcessFailedException;
	
	/**
	 * 
	 * @param flush
	 */
	void setFlushHandler(FlushHandler flush);
	/**
	 * 
	 * @param rule
	 */
	void setSurvivalRule(SurvivorRule rule);
}
