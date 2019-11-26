package io.reactiveminds.datagrid.util;

import java.util.function.Consumer;

import io.reactiveminds.datagrid.err.FlushFailedException;

@FunctionalInterface
public interface ThrowingLambdaConsumer<T, E extends Exception> {
	void accept(T t) throws E;
	public static <T> Consumer<T> throwsFlushException(ThrowingLambdaConsumer<T, Exception> throwingConsumer) {

		return i -> {
			try {
				throwingConsumer.accept(i);
			} 
			catch (Exception e) {
				throw new FlushFailedException(e.getMessage(), e);
			}
		};
	}
}


