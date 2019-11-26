package io.reactiveminds.datagrid.api.mock;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.reactiveminds.datagrid.api.FlushHandler;
import io.reactiveminds.datagrid.vo.KeyValRecord;
@Component
public class MockHandler implements FlushHandler {
	private static final Logger log = LoggerFactory.getLogger("MockHandler");
	public MockHandler() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int[] apply(List<KeyValRecord> flushRequests) {
		log.info("***~~~~|| Flushing ||~~~***");
		flushRequests.forEach(k -> System.out.println(k));
		int[] r = new int[flushRequests.size()];
		Arrays.fill(r, 0);
		return r;
	}

}
