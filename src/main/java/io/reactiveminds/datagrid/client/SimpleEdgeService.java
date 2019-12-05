package io.reactiveminds.datagrid.client;

import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.stop;
import static spark.Spark.threadPool;

import java.util.concurrent.locks.Lock;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import io.reactiveminds.datagrid.spi.ConfigRegistry;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.datagrid.vo.DataEvent;
import io.reactiveminds.datagrid.vo.GridCommand;
import io.reactiveminds.datagrid.vo.GridCommand.Command;
import spark.Request;
import spark.Response;
/**
 * Optional component. Should be moved to a dedicated edge node in client mode.
 * @author sdalui
 *
 */
@Component
public class SimpleEdgeService {

	private static final Logger log = LoggerFactory.getLogger("EdgeService");
	
	@Value("${coalesce.client.listenPort:8181}")
	private int port;
	@Value("${coalesce.client.maxThreads:8}")
	private int maxThreads;

	private Lock lock;
	@SuppressWarnings("deprecation")
	private void acquireLock() {
		lock = hz.getLock("GridClient");
		lock.lock();
	}
	@PostConstruct
	void init() {
		new Thread("Api.Listener.Acquirer") {
			@Override
			public void run() {
				initServer();
			}
		}.start();
	}
	
	private void initServer() {
		acquireLock();
		port(port);
		threadPool(maxThreads);
		init();
		
		post("/findByKey/:listenerCfg", (Request request, Response response) -> {
			
			log.info("Got request of type: "+request.contentType());
			String keyJson = request.body();
			String configName = request.params(":listenerCfg");
			
			String imap = configRegistry.getCoalesceMap(configName);
			Schema schm = configRegistry.getKeySchema(configName);
			
			if(imap != null && schm != null) {
				DataEvent data = get(imap, Utils.jsonToAvroRecord(schm, keyJson));
				if(data != null) {
					schm = configRegistry.getValueSchema(configName);
					GenericRecord val = Utils.fromAvroBytes(data.getMessageValue(), schm);
					response.type("application/json");
					response.status(200);
					return val.toString();
				}
			}
			response.status(404);
			return "";
		});
		
		post("/flush/:listenerCfg", (Request request, Response response) -> {
			
			String configName = request.params(":listenerCfg");
			flush(configName);
			response.status(201);
			return "OK";
			
		});

		log.info("Api server up and running on port "+port);
	}
	@PreDestroy
	void stopServer() {
		stop();
		lock.unlock();
	}
	@Autowired
	HazelcastInstance hz;
	public DataEvent get(String imap, GenericRecord key) {
		IMap<byte[], DataEvent> map = hz.getMap(imap);
		return map.get(Utils.toAvroBytes(key));

	}
	@Autowired
	ConfigRegistry configRegistry;
	public String search(String imap, String jsonPathQry) {
		throw new UnsupportedOperationException("TBD");
	}
	public void flush(String listenerCfg) {
		configRegistry.submitCommand(new GridCommand(Command.FLUSH, listenerCfg));
	}
}
