package io.reactiveminds.datagrid.api;

import org.apache.avro.generic.GenericRecord;
@FunctionalInterface
public interface SurvivorRule {
	/**
	 * Apply a survivor-ship rule. Return a null for records to be deleted
	 * @param onfile The current record into which the merge has to be done, or null if there is no current data (insert)
	 * @param request The incoming record which needs to be survived
	 * @return the updated/new record or null for deletion
	 */
	GenericRecord merge(GenericRecord onfile, GenericRecord request, GridContext grid);
}
