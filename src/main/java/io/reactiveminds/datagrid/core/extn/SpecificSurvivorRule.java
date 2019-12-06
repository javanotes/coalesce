package io.reactiveminds.datagrid.core.extn;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;

import io.reactiveminds.datagrid.api.GridContext;
import io.reactiveminds.datagrid.api.SurvivorRule;
import io.reactiveminds.datagrid.util.Utils;

public abstract class SpecificSurvivorRule implements SurvivorRule {

	protected abstract <V extends SpecificRecord> V doMerge(V onfile, V request, GridContext grid);
	@Override
	public GenericRecord merge(GenericRecord onfile, GenericRecord request, GridContext grid) {
		SpecificRecord merged = doMerge(Utils.genericToSpecific(onfile), Utils.genericToSpecific(request), grid);
		return Utils.specificToGeneric(merged);
	}

}
