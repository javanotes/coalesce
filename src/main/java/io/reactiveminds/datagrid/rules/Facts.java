package io.reactiveminds.datagrid.rules;

import org.apache.avro.specific.SpecificRecord;

public class Facts {

	public SpecificRecord getRequest() {
		return request;
	}
	public void setRequest(SpecificRecord request) {
		this.request = request;
	}
	public SpecificRecord getOnfile() {
		return onfile;
	}
	public void setOnfile(SpecificRecord onfile) {
		this.onfile = onfile;
	}
	SpecificRecord request;
	SpecificRecord onfile;
}
