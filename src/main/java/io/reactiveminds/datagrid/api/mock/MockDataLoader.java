package io.reactiveminds.datagrid.api.mock;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hazelcast.core.HazelcastInstance;

import io.reactiveminds.datagrid.api.DataLoader;
import io.reactiveminds.datagrid.spi.ConfigRegistry;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.mock.avro.IndividualTable;
@Component
public class MockDataLoader extends DataLoader {

	private static final Logger log = LoggerFactory.getLogger("MockDataLoader");
	public MockDataLoader() {
	}

	@Autowired
	MemberRepository repo;
	@Autowired
	ConfigRegistry config;
	
	@Override
	public GenericRecord load(GenericRecord key) {
		String pkey = config.getTracingId(key);
		Optional<Member> mem = repo.findById(pkey);
		if(mem.isPresent()) {
			Member member = mem.get();
			IndividualTable avro = IndividualTable.newBuilder()
			.setIndividualDocument(member.getDocument())
			.setIndividualId(member.getIndividualId())
			.setSourceCode(member.getSource())
			.setLoadDateTime(member.getLoadTime())
			.setIndividualAge(20)
			.setTrackingIdentifier(pkey)
			.build();
			
			log.debug("Fetched member from DB: "+member.getIndividualId());
			return Utils.specificToGeneric(avro);
		}
		return null;
		
	}

	
	@Override
	public Map<GenericRecord, GenericRecord> loadAll(List<GenericRecord> keys) {
		return null;
	}
	
	
	@Autowired
	HazelcastInstance hz;
	
	
}
