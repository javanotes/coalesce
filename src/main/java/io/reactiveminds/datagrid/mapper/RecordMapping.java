package io.reactiveminds.datagrid.mapper;

import java.util.ArrayList;
import java.util.List;

public class RecordMapping {
	
	public RecordMapping() {
	}
	public String getSourceAvro() {
		return sourceAvro;
	}


	public void setSourceAvro(String sourceAvro) {
		this.sourceAvro = sourceAvro;
	}


	public String getTargetAvro() {
		return targetAvro;
	}


	public void setTargetAvro(String targetAvro) {
		this.targetAvro = targetAvro;
	}


	public List<FieldMapping> getMapping() {
		return mapping;
	}


	public void setMapping(List<FieldMapping> mapping) {
		this.mapping = mapping;
	}

	private String name;
	private String sourceAvro;
	private String targetAvro;
	private List<FieldMapping> mapping = new ArrayList<>();
	private boolean strictTypeMapping = true;
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	public boolean isStrictTypeMapping() {
		return strictTypeMapping;
	}
	public void setStrictTypeMapping(boolean strictTypeMapping) {
		this.strictTypeMapping = strictTypeMapping;
	}
}
