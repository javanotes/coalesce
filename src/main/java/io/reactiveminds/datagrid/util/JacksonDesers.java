package io.reactiveminds.datagrid.util;

import java.io.IOException;

import org.apache.avro.Schema;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class JacksonDesers {

	private JacksonDesers() {
	}
	
	public static class SchemaDeser extends JsonDeserializer<Schema>{

		@Override
		public Schema deserialize(JsonParser p, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			String text = p.getText();
	        return new Schema.Parser().parse(ResourceUtils.getFile(text));
		}
		
	}
	
	public static class ClassDeser extends JsonDeserializer<Class<?>>{

		@Override
		public Class<?> deserialize(JsonParser p, DeserializationContext ctxt)
				throws IOException, JsonProcessingException {
			String text = p.getText();
			try {
				return Class.forName(text);
			} catch (ClassNotFoundException e) {
				throw new IOException(e);
			}
		}
		
	}

}
