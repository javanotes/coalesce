package io.reactiveminds.datagrid.api.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import io.reactiveminds.datagrid.api.FlushHandler;
import io.reactiveminds.datagrid.spi.ConfigRegistry;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.datagrid.vo.KeyValRecord;
import io.reactiveminds.mock.avro.IndividualTable;
import io.reactiveminds.mock.avro.IndividualTableKey;
@Component
public class MockHandler implements FlushHandler {
	private static final Logger log = LoggerFactory.getLogger("MockHandler");
	public MockHandler() {
		// TODO Auto-generated constructor stub
	}
	@Autowired
	ConfigRegistry config;
	@Autowired
	MemberRepository repo;
	private Member toModel(KeyValRecord  rec) {
		IndividualTableKey key = Utils.genericToSpecific(rec.getKey());
		IndividualTable tab = Utils.genericToSpecific(rec.getValue());
		
		Member m = new Member();
		m.setDocument(tab.getIndividualDocument());
		m.setId(config.getTracingId(rec.getKey()));
		m.setIndividualId(key.getIndividualId());
		m.setLoadTime(tab.getLoadDateTime());
		m.setSource(tab.getSourceCode());
		
		return m;
	}
	@Autowired
	NamedParameterJdbcOperations jdbc;
	@Value("${mock.flush.batchSize:50}")
	int batchSize;
	
	@Transactional
	@Override
	public int[] apply(List<KeyValRecord> flushRequests) {
		log.info("***~~~~|| Flushing ||~~~***");
		final List<Member> list = flushRequests.stream().map(r -> toModel(r)).collect(Collectors.toList());
		
		int iters = list.size() / batchSize;
		int rem = list.size() % batchSize;
		
		int i=1;
		int j = 0;
		int [] tmp;
		int[] result = new int[list.size()];
		List<Member> batch = new ArrayList<Member>(batchSize);
		
		for (; i <= iters; i++) {
			batch.clear();
			for (; j < i*batchSize; j++) {
				batch.add(list.get(j));
			}
			SqlParameterSource[] params = SqlParameterSourceUtils.createBatch(batch);
			tmp = jdbc.batchUpdate("insert into `member` (id,indv_id, source,document,load_time) values (:id, :individualId, :source, :document, :loadTime) "
					+ "on duplicate key update source=:source, document=:document, load_time=:loadTime", params);
			log.debug("Saved batch: "+i+", 'j' at: "+j);
			System.arraycopy(tmp, 0, result, j-batchSize, batchSize);
			
		}
		batch.clear();
		for (; j < list.size(); j++) {
			batch.add(list.get(j));
		}
		SqlParameterSource[] params = SqlParameterSourceUtils.createBatch(list);
		tmp = jdbc.batchUpdate("insert into `member` (id,indv_id, source,document,load_time) values (:id, :individualId, :source, :document, :loadTime) "
				+ "on duplicate key update source=:source, document=:document, load_time=:loadTime", params);
		System.arraycopy(tmp, 0, result, j-rem, rem);
		log.debug("Saved batch: "+i+", 'j' at: "+j+", rem: "+rem);
		
		return result;
	}

}
