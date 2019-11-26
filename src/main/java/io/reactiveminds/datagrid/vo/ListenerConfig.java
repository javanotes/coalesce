package io.reactiveminds.datagrid.vo;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.springframework.util.Assert;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.reactiveminds.datagrid.api.DataLoader;
import io.reactiveminds.datagrid.api.FlushHandler;
import io.reactiveminds.datagrid.api.SurvivorRule;
import io.reactiveminds.datagrid.err.ConfigurationException;
import io.reactiveminds.datagrid.util.JacksonDesers;
import io.reactiveminds.datagrid.util.ThrowingLambdaFunc;
import io.reactiveminds.datagrid.util.Utils;

public class ListenerConfig {
	
	/**
	 * 
	 * @param configFile
	 * @return
	 * @throws IOException
	 */
	public static ListenerConfig load(String configFile)  {
		try {
			return Utils.readJson(configFile, ListenerConfig.class);
		} catch (IOException e) {
			throw new ConfigurationException("Unable to load listener config", e);
		}
	}
	/**
	 * 
	 * @param configDir
	 * @return
	 */
	public static List<ListenerConfig> loadAll(String configDir) {
		try {
			return Files.walk(ResourceUtils.getFile(configDir).toPath())
			.filter(Files::isRegularFile)
			.map(ThrowingLambdaFunc.throwsConfigurationException(p -> Utils.readJson(new FileInputStream(p.toFile()), ListenerConfig.class), "Unable to read listener configuration"))
			.collect(Collectors.toList());
		} catch (IOException e) {
			throw new ConfigurationException("Unable to load listener config", e);
		}
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getCoalesceMap() {
		return coalesceMap;
	}
	public void setCoalesceMap(String coalesceMap) {
		this.coalesceMap = coalesceMap;
	}
	public String getRequestMap() {
		return requestMap;
	}
	public void setRequestMap(String requestMap) {
		this.requestMap = requestMap;
	}
	public String getRequestTopic() {
		return requestTopic;
	}
	public void setRequestTopic(String requestTopic) {
		this.requestTopic = requestTopic;
	}
	public Schema getKeySchema() {
		return keySchema;
	}
	public void setKeySchema(Schema keySchema) {
		this.keySchema = keySchema;
	}
	public Schema getValueSchema() {
		return valueSchema;
	}
	public void setValueSchema(Schema valueSchema) {
		this.valueSchema = valueSchema;
	}
	public Class<? extends DataLoader> getDataLoader() {
		return dataLoader;
	}
	public void setDataLoader(Class<? extends DataLoader> dataLoader) {
		this.dataLoader = dataLoader;
	}
	public ListenerConfig() {
	}
	String flushSchedule = "*/30 * * * * *";
	public String getFlushSchedule() {
		return flushSchedule;
	}
	public void setFlushSchedule(String flushSchedule) {
		this.flushSchedule = flushSchedule;
	}
	String name;
	String coalesceMap;
	String requestMap;
	String requestTopic;
	@JsonDeserialize(using = JacksonDesers.SchemaDeser.class)
	Schema keySchema;
	@JsonDeserialize(using = JacksonDesers.SchemaDeser.class)
	Schema valueSchema;
	@JsonDeserialize(using = JacksonDesers.ClassDeser.class)
	Class<? extends DataLoader> dataLoader;
	@JsonDeserialize(using = JacksonDesers.ClassDeser.class)
	Class<? extends FlushHandler> flushHandler;
	@JsonDeserialize(using = JacksonDesers.ClassDeser.class)
	Class<? extends SurvivorRule> survivalRule;
	public Class<? extends SurvivorRule> getSurvivalRule() {
		return survivalRule;
	}
	public void setSurvivalRule(Class<? extends SurvivorRule> survivalRule) {
		this.survivalRule = survivalRule;
	}
	public Class<? extends FlushHandler> getFlushHandler() {
		return flushHandler;
	}
	public void setFlushHandler(Class<? extends FlushHandler> flushHandler) {
		this.flushHandler = flushHandler;
	}
	
	@Override
	public String toString() {
		return "ListenerConfig [name=" + name + ", coalesceMap=" + coalesceMap + ", requestMap=" + requestMap
				+ ", requestTopic=" + requestTopic + ", keySchema=" + keySchema + ", valueSchema=" + valueSchema
				+ ", dataLoader=" + dataLoader + ", flushHandler=" + flushHandler + ", survivalRule=" + survivalRule
				+ "]";
	}
	public void assertNotNull() {
		Assert.notNull(dataLoader, "dataLoader is null");
		Assert.notNull(flushHandler, "flushHandler is null");
		Assert.notNull(survivalRule, "survivalRule is null");
		Assert.notNull(coalesceMap, "coalesceMap is null");
		Assert.notNull(requestMap, "requestMap is null");
		//Assert.notNull(requestTopic, "requestTopic is null");
		Assert.notNull(keySchema, "keySchema is null");
		Assert.notNull(valueSchema, "valueSchema is null");
		
	}
}
