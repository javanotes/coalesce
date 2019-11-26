package io.reactiveminds.datagrid.mapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import javax.script.ScriptException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import io.reactiveminds.datagrid.util.ThrowingLambdaConsumer;
import io.reactiveminds.datagrid.util.Utils;
/**
 * Simple Avro record mapper. Expects the source and target schema mapping types to be same.
 * @author sdalui
 *
 */
public class AvroRecordMapper {
	
	private static List<String> extractDerivedParams(String input) {
		List<String> params = new ArrayList<String>();
		String str = input;
		
		while(!str.isEmpty() && Utils.hasEnclosingDelim(str, JSUtil.DOLLAR+JSUtil.OPEN, JSUtil.CLOSE)) {
			String p = Utils.extractWithinEnclosingDelim(str, JSUtil.DOLLAR+JSUtil.OPEN, JSUtil.CLOSE);
			if(Utils.hasText(p))
				params.add(p);
			
			String arg = JSUtil.DOLLAR+JSUtil.OPEN+p+JSUtil.CLOSE;
			int i = str.indexOf(arg);
			str = str.substring(i+arg.length());
		}
		return params;
	}
	/**
	 * Create new mapper with a given mapping 
	 * @param mapping
	 */
	private AvroRecordMapper(RecordMapping mapping) {
		super();
		this.mapping = mapping;
	}
	/**
	 * For testing
	 * @param mapping
	 * @return
	 */
	static Schema getSourceSchema(String mapping) {
		if(registry.containsKey(mapping)) {
			return registry.get(mapping).getSourceSchema();
		}
		return null;
	}
	/**
	 * For testing
	 * @param mapping
	 * @return
	 */
	static Schema getTargetSchema(String mapping) {
		if(registry.containsKey(mapping)) {
			return registry.get(mapping).getTargetSchema();
		}
		return null;
	}
	/**
	 * Register a new mapping by given name. Will override if anything present already with same name.
	 * @param mappingConfigFile
	 * @return 
	 * @return
	 * @throws IOException if validation fails
	 */
	public static synchronized String registerMapping(File mappingConfigFile) throws MappingException {
		RecordMapping mapping = null;
		try {
			mapping = Utils.readJson(new FileInputStream(mappingConfigFile), RecordMapping.class);
			Utils.assertHasText(mapping.getName(), mappingConfigFile+", Mapping name not set");
			Utils.assertHasText(mapping.getSourceAvro(), mappingConfigFile+", Source avro path not set");
			Utils.assertHasText(mapping.getTargetAvro(), mappingConfigFile+", Target avro path not set");
		} 
		catch (IOException e) {
			throw new MappingException("Unable to read mapping config file", e);
		}
		catch (IllegalArgumentException e) {
			throw new MappingException(e.getMessage());
		}
		AvroRecordMapper mapper = new AvroRecordMapper(mapping);
		try {
			mapper.load();
		} catch (IOException e) {
			throw new MappingException("Unable to load mapping: "+e.getMessage(), e);
		}
		if(mapper.isLoaded()) {
			registry.put(mapping.getName(), mapper);
			return mapping.getName();
		}
		else
			throw new MappingException("Unable to load mapping. Is config file empty?");
	}
	private static Map<String, AvroRecordMapper> registry = new ConcurrentHashMap<String, AvroRecordMapper>();
	private Schema sourceSchema, targetSchema;
	Schema getTargetSchema() {
		return targetSchema;
	}
	private boolean loaded = false;
	private final RecordMapping mapping;
	Schema getSourceSchema() {
		return sourceSchema;
	}
	/**
	 * 
	 * @param in
	 * @param elem
	 * @return
	 */
	private Object get0(GenericRecord in, String elem) {
		String filter = Utils.extractWithinEnclosingDelim(elem, "[", "]");
		
		if(Utils.hasText(filter)) {
			String [] args = filter.split(",");
			if(args.length == 2) {
				if(in.getSchema().getType() != Type.ARRAY)
					throw new IllegalArgumentException("filter allowed for ARRAY elements only: "+elem);
				
				if(in.getSchema().getElementType().getType() == Type.RECORD) {
					GenericArray<?> arr = (GenericArray<?>) in;
					for(Object rec : arr){
						GenericRecord item = (GenericRecord) rec;
						
						if(TypeHandler.equalsTo(item.get(args[0]), args[1], item.getSchema().getField(args[0]).schema())) {
							return item;//return record
						}
					}
				}
				else {
					GenericArray<?> arr = (GenericArray<?>) in;
					for(Object rec : arr){
						
						if(TypeHandler.equalsTo(rec, args[1], arr.getSchema().getElementType())) {
							return rec;//return item
						}
					}
				}
				
			}
			else
				throw new IllegalArgumentException("invalid filter syntax: "+elem);
		}
		else {
			return in.get(elem);
		}
		
		return null;
	}
	
	
	@SuppressWarnings("unchecked")
	private void set0(GenericRecord in, String elem, Object val) {
		String filter = Utils.extractWithinEnclosingDelim(elem, "[", "]");
		
		if(Utils.hasText(filter)) {
			
			String [] whereAndValue = filter.split(",");
			if(whereAndValue.length == 3) {
				if(in.getSchema().getType() != Type.ARRAY)
					throw new IllegalArgumentException("filter allowed for ARRAY elements only: "+elem);
				
				if(in.getSchema().getElementType().getType() == Type.RECORD) {
					GenericArray<?> arr = (GenericArray<?>) in;
					for(Object rec : arr){
						GenericRecord item = (GenericRecord) rec;
						
						if(TypeHandler.equalsTo(item.get(whereAndValue[0]), whereAndValue[1], item.getSchema().getField(whereAndValue[0]).schema())) {
							//TypeHandler.resolve(from, to, whereAndValue)
							item.put(whereAndValue[2], val);
							return;
						}
					}
				}
				else {
					@SuppressWarnings("rawtypes")
					GenericArray arr = (GenericArray<?>) in;
					int i=0;
					for(Object rec : arr){
						
						if(TypeHandler.equalsTo(rec, whereAndValue[1], arr.getSchema().getElementType())) {
							arr.set(i, val);
							return;
						}
						i++;
					}
				}
				
			}
			else
				throw new IllegalArgumentException("invalid filter syntax: "+elem);
		}
		else {
			in.put(elem, val);
		}
		
	}
	void set(GenericRecord in, String path, Object val) {
		String []args = path.split("\\.");
		if(args.length == 1) {
			set0(in, args[0], val);
			return;
		}
		else {
			GenericRecord inin = (GenericRecord) get0(in, args[0]);
			set(inin, path.substring(args[0].length()+1), val);
		}
	}
	Object get(GenericRecord in, String path) {
		String []args = path.split("\\.");
		if(args.length == 1) {
			return get0(in, args[0]);
		}
		else {
			GenericRecord inin = (GenericRecord) get0(in, args[0]);
			return get(inin, path.substring(args[0].length()+1));
		}
	}
	private static Object evalParam(GenericRecord in, AvroRecordMapper mapper, FieldMapping fm) {
		Object o = mapper.get(in, fm.getSourceField());
		if(Utils.hasText(fm.getExpr())) {
			try {
				o = JSUtil.evalExpr(fm.getExpr(), o);
			} catch (ScriptException e) {
				throw new MappingException(fm.toString(), e);
			}
		}
		return o;
	}
	private static Object evalDerivedParam(GenericRecord in, AvroRecordMapper mapper, FieldMapping fm) {
		Utils.assertHasText(fm.getExpr(), "Expression missing in derived field mapping");
		List<String> fields = extractDerivedParams(fm.getExpr());
		Utils.assertIsTrue(!fields.isEmpty(), "No field reference found in derived expr");
		
		Map<String, Object> params = new HashMap<>();
		fields.forEach(f -> params.put(f, mapper.get(in, f)));
		try {
			return JSUtil.evalDerivedExpr(params, fm.getExpr());
		} catch (ScriptException e) {
			throw new MappingException(fm.toString(), e);
		}
	}
	/**
	 * 
	 * @param <T>
	 * @param throwingConsumer
	 * @return
	 */
	private static <T> Consumer<T> throwingLambda(ThrowingLambdaConsumer<T, Exception> throwingConsumer) {

		return i -> {
			try {
				throwingConsumer.accept(i);
			} catch (IllegalArgumentException e) {
				throw new MappingException(e.getMessage());
			}
			catch (Exception e) {
				if(e instanceof MappingException)
					throw ((MappingException)e);
				throw new MappingException(e.getMessage(), e);
			}
		};
	}
	
	/**
	 * The core transform method. Will return a transformed Avro. If mappingName is not found, throws an exception
	 * if there is no match.
	 * @param in
	 * @return
	 */
	public static GenericRecord map(GenericRecord in, GenericRecord out, String mappingName) throws MappingException {
		if(registry.containsKey(mappingName)) {
			AvroRecordMapper mapper = registry.get(mappingName);
			if(mapper.sourceSchema.equals(in.getSchema())) {
				
				mapper.mapping.getMapping().forEach(throwingLambda (fm -> {
					
					try {
						Object o = fm.isDerived() ? evalDerivedParam(in, mapper, fm) : evalParam(in, mapper, fm);
						mapper.set(out, fm.getTargetField(), o);
					} catch (IllegalArgumentException e) {
						throw new MappingException(e.getMessage());
					}
					catch (Exception e) {
						if(e instanceof MappingException)
							throw e;
						throw new MappingException(e.getMessage(), e);
					}
				}) );
				return out;
			}
			else
				throw new MappingException(mappingName+" => input source schema mismatch. expect: "+mapper.sourceSchema.getFullName()+", found: "+in.getSchema().getFullName());
		}
		throw new MappingException(mappingName + " => mapping not found");
	}
	/**
	 * Simply returns the input Avro, in case a mapping cannot be found in registry. This is the difference 
	 * with the other map() method that throws an exception if mapping name not found.
	 * @param in
	 * @param mappingName
	 * @return
	 * @throws MappingException
	 */
	public static GenericRecord map(GenericRecord in, String mappingName) throws MappingException {
		if(registry.containsKey(mappingName)) {
			AvroRecordMapper mapper = registry.get(mappingName);
			Schema target = mapper.getTargetSchema();
			GenericRecord out = new GenericData.Record(target);
			return map(in, out, mappingName);
		}
		return in;
		//throw new MappingException(mappingName + " => mapping not found");
	}
	
	/**
	 * Load the mapping structure with initial validations. Expecting the field schema types to match.
	 * @throws IOException
	 */
	private void load() throws IOException {
		sourceSchema = new Schema.Parser().parse(Utils.getFile(mapping.getSourceAvro()));
		targetSchema = new Schema.Parser().parse(Utils.getFile(mapping.getTargetAvro()));
		
		try {
			Utils.assertIsTrue(sourceSchema.getType() == Type.RECORD, mapping.getSourceAvro()+" - source schema not of type RECORD");
			Utils.assertIsTrue(targetSchema.getType() == Type.RECORD, mapping.getTargetAvro()+" - target schema not of type RECORD");
		} 
		catch (IllegalArgumentException e) {
			throw new IOException("["+mapping.getName()+"] "+e.getMessage());
		}
		Map<String, Schema> sourceFldMap = sourceSchema.getFields().stream().collect(Collectors.toMap(Field::name, Field::schema));
		Map<String, Schema> targetFldMap = targetSchema.getFields().stream().collect(Collectors.toMap(Field::name, Field::schema));
		
		if(mapping.getMapping() != null && !mapping.getMapping().isEmpty()) {
			for(FieldMapping each : mapping.getMapping()) {
				if (!each.isDerived()) {
					try {
						Type s = checkFieldPath(sourceFldMap, each.getSourceField());
						Type t = checkFieldPath(targetFldMap, each.getTargetField());

						if (mapping.isStrictTypeMapping())
							Utils.assertIsTrue(s == t, each.getSourceField() + ", " + each.getTargetField()
									+ " - source/target field type mismatch. source: " + s + ", target: " + t);
						else
							Utils.assertIsTrue(s == t || (TypeHandler.isCompatible(s, t)),
									each.getSourceField() + ", " + each.getTargetField()
											+ " - source/target field type incompatible. source: " + s + ", target: "
											+ t);
					} catch (IllegalArgumentException e) {
						throw new IOException("[" + mapping.getName() + "] " + e.getMessage());
					} 
				}
				else {
					Utils.assertHasText(each.getExpr(), each.getTargetField()+" is a derived field. requires an expression to be evaluated using '${}' syntax");
				}
			}
		}
		
		setLoaded(true);
	}
	private static Type checkFieldPath(Map<String, Schema> fldMap, String fld) {
		String[] flds = fld.split("\\.");
		if(flds.length == 1) {
			Utils.assertIsTrue(fldMap.containsKey(fld), fld+" - field not defined in schema");
			return fldMap.get(fld).getType();
		}
		else {
			Utils.assertIsTrue(fldMap.containsKey(flds[0]), flds[0]+" - parent field not defined in schema");
			Schema parent = fldMap.get(flds[0]);
			
			Map<String, Schema> childFldMap = null;
			if(parent.getType() == Type.RECORD) {
				childFldMap = parent.getFields().stream().collect(Collectors.toMap(Field::name, Field::schema));
			}
			else if(parent.getType() == Type.UNION) {
				//for UNION, type is RECORD, iterate through all records
				childFldMap = parent.getTypes().stream().map(Schema::getFields).flatMap(List::stream).collect(Collectors.toMap(Field::name, Field::schema));
			}
			else
				throw new IllegalArgumentException(flds[0]+" - parent field not of type record");
			
			return checkFieldPath(childFldMap, fld.substring(flds[0].length()+1));
		}
	}
	boolean isLoaded() {
		return loaded;
	}
	private void setLoaded(boolean loaded) {
		this.loaded = loaded;
	}
}
