package io.reactiveminds.datagrid.util;


import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;

import io.confluent.avro.random.generator.Generator;
import io.reactiveminds.datagrid.err.ConfigurationException;
import io.reactiveminds.mock.avro.IndividualTable;

public class Utils {
	@SuppressWarnings("unchecked")
	public static <T extends SpecificRecord> T genericToSpecific(GenericRecord g) {
		return (T) SpecificData.get().deepCopy(g.getSchema(), g);
	}
	public static GenericRecord specificToGeneric(SpecificRecord s){
		return (GenericRecord) GenericData.get().deepCopy(s.getSchema(), s);
	}
	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
	
	private static String toHexString(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
	    for (int j = 0; j < bytes.length; j++) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = HEX_ARRAY[v >>> 4];
	        hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
	    }
	    return new String(hexChars);
	}
	/**
	 * 
	 * @param record
	 * @return
	 */
	public static String generateValueChecksum(byte[] record) {
		return generateDigest(DIGEST_SHA256, record);
	}
	public static String generateKeyChecksum(byte[] record) {
		return generateDigest(DIGEST_MD5, record);
	}
	public static String generateDigest(GenericRecord key, String algo) {
		return generateDigest(algo, toAvroBytes(key));
	}
	public static final String DIGEST_MD5 = "MD5";
	public static final String DIGEST_SHA256 = "SHA-256";
	/**
	 * 
	 * @param algo
	 * @param b
	 * @return
	 */
	public static String generateDigest(String algo, byte[] b) {
		try {
			MessageDigest md = MessageDigest.getInstance(algo);
			md.update(b);
			return toHexString(md.digest());
		} catch (NoSuchAlgorithmException e) {
			throw new ConfigurationException(e.getMessage());
		}
	}
	private static void setschemaProps(Schema s) {
		if(s.getType() == Type.RECORD) {
			s.getFields().forEach(f -> setschemaProps(f.schema()));
		}
		else if(s.getType() == Type.UNION) {
			s.getTypes().forEach(e -> setschemaProps(e));
		}
		else if(s.getType() == Type.ARRAY) {
			setschemaProps(s.getElementType());
		}
		else {
			Map<String, String> args = new  HashMap<String, String>();
			args.put(Generator.REGEX_PROP, REGEX_ALPHANUM);
			s.addProp(Generator.ARG_PROPERTIES_PROP, args);
			
			return;
		}
	}
	public static GenericRecord randomAvroRecord(Schema s) {
		setschemaProps(s);
		Generator g = new Generator(s, new Random());
		return (GenericRecord) g.generate();
	}
	
	public static String sampleJson(String path) {
		try {
			File f = ResourceUtils.getFile(path);
			return jackson.readTree(f).toString();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "{}";
	}
	
	public static boolean isJsonFile(File f) {
		return f.exists() && f.isFile() && "json".equalsIgnoreCase(StringUtils.getFilenameExtension(f.getName()));
	}
	public static boolean isPatternMatched(File f, String pattern) {
		return StringUtils.hasText(pattern) ? f.getName().matches(pattern) : true;
	}
	
	/**
	 * 
	 * @param schema
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public static GenericRecord jsonToAvroRecord(Schema schema, String input) throws IOException {
		//Ref: https://issues.apache.org/jira/browse/AVRO-1582
		Decoder dec = new ExtendedJsonDecoder(schema, input);
		//BinaryDecoder dec = DecoderFactory.get().binaryDecoder(input.getBytes(StandardCharsets.UTF_8), null);
		DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
		return reader.read(null, dec);
	}
	/**
	 * Generate avro schema for the given class
	 * @param type
	 * @return
	 */
	public static Schema generateAvroSchema(Class<?> type) {
		ObjectMapper mapper = new ObjectMapper(new AvroFactory());
		AvroSchemaGenerator gen = new AvroSchemaGenerator();
		try {
			mapper.acceptJsonFormatVisitor(type, gen);
		} catch (JsonMappingException e) {
			throw new UncheckedIOException(e);
		}
		AvroSchema schemaWrapper = gen.getGeneratedSchema();
		return schemaWrapper.getAvroSchema();
	}
	static ObjectMapper jackson = new ObjectMapper();
	static {
		jackson.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}
	public static String toPrettyJsonString(Object v) {
		try {
			return jackson.writerWithDefaultPrettyPrinter().writeValueAsString(v);
		} catch (JsonProcessingException e) {
			throw new UncheckedIOException(e);
		}
		
	}
	/**
	 * 
	 * @param <T>
	 * @param jsonFile
	 * @param ofType
	 * @return
	 * @throws IOException 
	 */
	public static <T> T readJson(InputStream jsonSrc, Class<T> ofType) throws IOException {
		try {
			return jackson.readerFor(ofType).readValue(jsonSrc);
		} catch (IOException e) {
			throw e;
		}
		
	}
	public static <T> T readJson(String jsonSrcFile, Class<T> ofType) throws IOException {
		try {
			return readJson(new FileInputStream(ResourceUtils.getFile(jsonSrcFile)), ofType);
		} catch (IOException e) {
			throw e;
		}
		
	}
	/**
	 * May or may not be required
	 * @param genericRecord
	 * @return
	 * @throws IOException
	 */
	public static String avroRecordtoJson(GenericRecord genericRecord) throws IOException  {
		JsonNode n = jackson.readTree(genericRecord.toString());
		return jackson.writerWithDefaultPrettyPrinter().writeValueAsString(n);
		/**
	    */
	}
	public static String encodeToJson(GenericRecord genericRecord) {
		try(ByteArrayOutputStream baos = new ByteArrayOutputStream()){
	    	DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(genericRecord.getSchema());
		    JsonEncoder encoder = EncoderFactory.get().jsonEncoder(genericRecord.getSchema(), baos, false);
		    writer.write(genericRecord, encoder);
		    encoder.flush();
		    return new  String(baos.toByteArray(), StandardCharsets.UTF_8);
	    }
		catch(IOException e) {
			throw new UncheckedIOException("encode to json", e);
		}
	}
	/**
	 * Convert an Avro record to byte stream
	 * @param genericRecord
	 * @return
	 */
	public static byte[] toAvroBytes(GenericRecord genericRecord) {
		try(ByteArrayOutputStream baos = new ByteArrayOutputStream()){
	    	DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(genericRecord.getSchema());
		    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
		    writer.write(genericRecord, encoder);
		    encoder.flush();
		    return baos.toByteArray();
	    }
		catch(IOException e) {
			throw new UncheckedIOException("encode to bytes", e);
		}
	}
	/**
	 * Reconstruct avro record from bytes
	 * @param b
	 * @param s
	 * @return
	 */
	public static GenericRecord fromAvroBytes(byte[] b, Schema s) {
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(b, null);
		DatumReader<GenericRecord> reader = new GenericDatumReader<>(s);
        try {
			return reader.read(null, decoder);
		} catch (IOException e) {
			throw new UncheckedIOException("decode from bytes", e);
		}
	}
	
	public static void main(String[] args) throws IOException {
		GenericRecord rec = randomAvroRecord(loadSchema("classpath:avro/eldm_individual.avsc"));
		System.out.println(rec.toString());
		IndividualTable ind = genericToSpecific(rec);
		System.out.println(ind.getIndividualId());
		
		GenericRecord rec2 = specificToGeneric(ind);
		System.out.println(rec2.toString());
	}
	
	public static Schema loadSchema(String file) throws IOException {
		Schema.Parser p = new Schema.Parser();
		return p.parse(ResourceUtils.getFile(file));
	}
	/**
	 * Whether an enclosing delimiter is present
	 * @param in
	 * @param startChar
	 * @param endChar
	 * @return
	 */
	public static boolean hasEnclosingDelim(String in, String startChar, String endChar) {
		return hasEnclosingDelim0(in, startChar, endChar) != null;
	}
	private static String hasEnclosingDelim0(String in, String startChar, String endChar) {
		int i1 = in.indexOf(startChar);
		int i2 = in.indexOf(endChar);
		 
		if(i1 != -1 && i2 != -1 && i2 > i1) {
			return i1 + " " + i2;
		}
		return null;
	}
	/**
	 * 
	 * @param in
	 * @param startChar
	 * @param endChar
	 * @return
	 */
	public static String extractWithinEnclosingDelim(String in, String startChar, String endChar) {
		String idx = hasEnclosingDelim0(in, startChar, endChar);
		if(idx != null) {
			String [] idxArr = idx.split(" ");
			return in.substring(Integer.parseInt(idxArr[0])+startChar.length(), Integer.parseInt(idxArr[1]));
		}
		
		return "";
	}
	public static final String REGEX_ALPHANUM = "[a-zA-Z0-9]+";
	/**
	 * 
	 * @param type
	 * @return
	 */
	public static Schema classToSchema(Class<?> type) {
		return ReflectData.get().getSchema(type);
	}
	public static <T> GenericRecord pojoToRecordAvro(T model) throws IOException {
        Schema schema = classToSchema(model.getClass());
        ReflectDatumWriter<T> datumWriter = new ReflectDatumWriter<>(schema);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
 
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        datumWriter.write(model, encoder);
        encoder.flush();
 
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(outputStream.toByteArray(), null);
 
        return datumReader.read(null, decoder);
    }
	public static <T> GenericRecord pojoToRecordJackson(T model) throws IOException {
        Schema schema = generateAvroSchema(model.getClass());
        return jsonToAvroRecord(schema, jackson.writerFor(model.getClass()).writeValueAsString(model));
    }
	
	/**
	 * 
	 * @param schema
	 * @return
	 */
	public static GenericRecord newRecord(File schema) {
		GenericRecord rec = null;
		try {
			rec = new GenericData.Record(new Schema.Parser().parse(schema));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
		return rec;
	}
	public static boolean hasText(String p) {
		return StringUtils.hasText(p);
	}
	/**
	 * 
	 * @param input
	 * @param msg
	 */
	public static void assertHasText(String input, String msg) {
		if(!hasText(msg))
			throw new IllegalArgumentException(msg);
	}
	/**
	 * 
	 * @param b
	 * @param msg
	 */
	public static void assertIsTrue(boolean b, String msg) {
		if(!b)
			throw new IllegalArgumentException(msg);
	}
	public static File getFile(String resourceLocation) throws IOException {
		return ResourceUtils.getFile(resourceLocation);
	}
}
