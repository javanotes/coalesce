package io.reactiveminds.datagrid.err;

import java.io.IOException;
import java.io.UncheckedIOException;

public class FlushFailedException extends UncheckedIOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public FlushFailedException(String msg) {
		super(new IOException(msg));
	}

	public FlushFailedException(String msg, IOException cause) {
		super(msg, cause);
	}

}
