package io.reactiveminds.datagrid.api.mock;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;

import io.reactiveminds.datagrid.api.SurvivorRule;
import io.reactiveminds.datagrid.err.ProcessFailedException;
import io.reactiveminds.datagrid.spi.GridContext;
import io.reactiveminds.datagrid.util.Utils;
import io.reactiveminds.mock.avro.IndividualTable;

@Component
public class MockRule implements SurvivorRule {

	private static final Logger log = LoggerFactory.getLogger("MockRule");
	public MockRule() {
		// TODO Auto-generated constructor stub
	}

	static {
		Configuration.setDefaults(new Configuration.Defaults() {

		    private final JsonProvider jsonProvider = new JacksonJsonProvider();
		    private final MappingProvider mappingProvider = new JacksonMappingProvider();
		      
		    @Override
		    public JsonProvider jsonProvider() {
		        return jsonProvider;
		    }

		    @Override
		    public MappingProvider mappingProvider() {
		        return mappingProvider;
		    }
		    
		    @Override
		    public Set<Option> options() {
		        return EnumSet.noneOf(Option.class);
		    }
		});
	}
	public static final String JPATH_MEM_EFF_DATE = "$.membershipEffectiveDate.date";
	public static final String JPATH_CUST_PUR_ID = "$.customerPurchaseIdentifier.id";
	static final String MEM_EFF_NODE = "membershipEffectiveDate";
	static final String CUST_PUR_NODE = "customerPurchaseIdentifier";
	
	static void setEffectiveDt(JsonNode doc, String date) {
		JsonNode node = doc.get(MEM_EFF_NODE);
		if(node != null) {
			ObjectNode o = (ObjectNode) node;
			o.put("date", date);
		}
	}
	static void setCustPurId(JsonNode doc, String id) {
		JsonNode node = doc.get(CUST_PUR_NODE);
		if(node != null) {
			ObjectNode o = (ObjectNode) node;
			o.put("id", id);
		}
	}
	
	//public static final String JPATH_MEM_EFF_DATE = "$.membershipEffectiveDate.date";
	//public static final String JPATH_MEM_EFF_DATE = "$.membershipEffectiveDate.date";
	//public static final String JPATH_MEM_EFF_DATE = "$.membershipEffectiveDate.date";
	
	@Override
	public GenericRecord merge(GenericRecord base, GenericRecord in, GridContext grid) {
		log.debug("Survivor rule applied: "+in);
		
		final IndividualTable current = Utils.genericToSpecific(base);
		final IndividualTable incoming = Utils.genericToSpecific(in);
		
		
		try {
			Object currDoc = Configuration.defaultConfiguration().jsonProvider().parse(current.getIndividualDocument());
			Object incDoc = Configuration.defaultConfiguration().jsonProvider().parse(incoming.getIndividualDocument());
			
			ObjectMapper om = new ObjectMapper();
			JsonNode tree = om.readTree(current.getIndividualDocument());
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date old = sdf.parse(JsonPath.read(currDoc, JPATH_MEM_EFF_DATE));
			Date now = sdf.parse(JsonPath.read(incDoc, JPATH_MEM_EFF_DATE));
			
			//if the incoming memb_eff_date later, then update cust_pur_id
			if(now.after(old)) {
				JsonNode node = tree.get("customerPurchaseIdentifier");
				if(node != null) {
					ObjectNode o = (ObjectNode) node;
					o.put("id", JsonPath.read(incDoc, JPATH_CUST_PUR_ID).toString());
				}
				current.setIndividualDocument(tree.toString());
			}
			
		} 
		catch (Exception e) {
			throw new ProcessFailedException("merge: ", e);
		}
		
		
		return Utils.specificToGeneric(current);
	}

}
