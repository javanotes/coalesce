package io.reactiveminds.datagrid.api.mock;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import io.reactiveminds.datagrid.api.FlushHandler;
import io.reactiveminds.datagrid.vo.KeyValRecord;

public class JdbcFlushHandler implements FlushHandler {

	String insertQuery;
	String[] keyMapping;
	Schema valueSchema;
	int batchSize;
	@Autowired
	JdbcTemplate template;
	
	private class ArgsBinder implements BatchPreparedStatementSetter{

		public ArgsBinder(List<KeyValRecord> flushRequests) {
			super();
			this.flushRequests = flushRequests;
		}

		private final List<KeyValRecord> flushRequests;
		@Override
		public void setValues(PreparedStatement ps, int i) throws SQLException {
			KeyValRecord rec = flushRequests.get(i);
			int j=0;
			for (; j < keyMapping.length; j++) {
				ps.setObject(j+1, rec.getKey().get(keyMapping[j]));
			}
			ps.setObject(j, rec.getValue().toString());
		}

		@Override
		public int getBatchSize() {
			return Math.min(batchSize, flushRequests.size());
		}
		
	}
	
	@PostConstruct
	void init() throws SQLException {
	}
	
	@Override
	public int[] apply(List<KeyValRecord> flushRequests) {
		return template.batchUpdate(insertQuery, new ArgsBinder(flushRequests));
	}

}
