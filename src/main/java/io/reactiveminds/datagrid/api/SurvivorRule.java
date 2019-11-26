package io.reactiveminds.datagrid.api;

import org.apache.avro.generic.GenericRecord;

import io.reactiveminds.datagrid.spi.GridContext;
@FunctionalInterface
public interface SurvivorRule {
	/**
	 * Apply a survivor-ship rule
	 * @param base The current record into which the merge has to be done
	 * @param in The incoming record which needs to be survived
	 * @return
	 */
	GenericRecord merge(GenericRecord base, GenericRecord in, GridContext grid);
}
