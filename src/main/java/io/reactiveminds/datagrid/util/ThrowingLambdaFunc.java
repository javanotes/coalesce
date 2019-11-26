package io.reactiveminds.datagrid.util;

import java.util.function.Function;

import io.reactiveminds.datagrid.err.ConfigurationException;

@FunctionalInterface
public interface ThrowingLambdaFunc<T, R, E extends Exception> {
	R accept(T t) throws E;
	
	public static <T,R> Function<T, R> throwsConfigurationException(ThrowingLambdaFunc<T, R, Exception> throwingConsumer, String errMsg) {

		return i -> {
			try {
				return throwingConsumer.accept(i);
			} 
			catch (Exception e) {
				throw new ConfigurationException(errMsg, e);
			}
		};
	}
}


