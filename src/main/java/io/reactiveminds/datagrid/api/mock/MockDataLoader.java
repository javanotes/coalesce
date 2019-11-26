package io.reactiveminds.datagrid.api.mock;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;

import io.reactiveminds.datagrid.api.DataLoader;
import io.reactiveminds.datagrid.spi.IngestionService;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.mock.avro.IndividualTable;
@Component
public class MockDataLoader extends DataLoader implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger("MockDataLoader");
	public MockDataLoader() {
	}

	private Schema valSchema;
	private Schema keySchema;
	
	@Value("${mock.keySchemaPath:classpath:avro/key.avsc}")
	String keySchemaPath;
	@Value("${mock.valSchemaPath:classpath:avro/eldm_individual.avsc}")
	String valSchemaPath;
	
	private String json;
	
	@PostConstruct
	void init() throws IOException {
		valSchema = Utils.loadSchema(valSchemaPath);
		json = Utils.sampleJson("classpath:avro/pref.json");
	}
	@Override
	public GenericRecord load(GenericRecord key) {
		return null;
		/*
		 * GenericRecord rec = valueForKey(key); log.info("LOAD: "+rec); return rec;
		 */
	}

	private GenericRecord valueForKey(GenericRecord key) {
		GenericRecord rec = Utils.randomAvroRecord(valSchema);
		rec.put(VAL_PROP, key.get(KEY_PROP));
		return rec;
	}
	@Override
	public Map<GenericRecord, GenericRecord> loadAll(List<GenericRecord> keys) {
		return null;
	}
	
	@Autowired
	Environment env;
	@Autowired
	HazelcastInstance hz;
	
	static String KEY_PROP = "individual_id";
	static String VAL_PROP = "individual_id";
	
	private static GenericRecord key(String k, Schema s) {
		GenericRecord r = new GenericData.Record(s);
		r.put(KEY_PROP, k);
		return r;
	}
	@Autowired
	IngestionService ingest;
	@Override
	public void run(String... args) throws Exception {
		
		keySchema = new Schema.Parser().parse(ResourceUtils.getFile(keySchemaPath));
		String requestMap = env.getProperty("mock.requestMap", "individualStage");
		
		int uniqKeys = env.getProperty("mock.key.uniqueSet", Integer.TYPE, 100);
		int maxItems = env.getProperty("mock.value.maxTotal", Integer.TYPE, 100000);
		
		//IMap<byte[], byte[]> map = hz.getMap(requestMap);
		Random r = new Random();
		ObjectMapper om = new ObjectMapper();
		
		LocalDate now = LocalDate.now();
		DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		
		for (int i = 0; i < maxItems; i++) {
			GenericRecord k = key(""+r.nextInt(uniqKeys), keySchema);
			GenericRecord v = valueForKey(k);
			
			IndividualTable ind = Utils.genericToSpecific(v);
			JsonNode doc = om.readTree(json);
			MockRule.setCustPurId(doc, now.format(sdf)+"__"+i);
			MockRule.setEffectiveDt(doc, now.format(sdf));
			ind.setIndividualDocument(doc.toString());
			
			ingest.apply(requestMap, k, Utils.specificToGeneric(ind), System.currentTimeMillis());
			now = now.plusDays(1);
		}
	}

}
