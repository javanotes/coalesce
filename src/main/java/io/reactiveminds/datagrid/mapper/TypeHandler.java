package io.reactiveminds.datagrid.mapper;

import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

class TypeHandler {

	/**
	 * 
	 * @param val
	 * @param literal
	 * @param schema
	 * @return
	 */
	public static boolean equalsTo(Object val, String literal, Schema schema) {
		try {
			switch(schema.getType()) {
			
				case BOOLEAN:
					return val.equals(Boolean.parseBoolean(literal));
				case DOUBLE:
					return val.equals(Double.parseDouble(literal));
				case ENUM:
					return val.toString().equals(literal);
				case FLOAT:
					return val.equals(Float.parseFloat(literal));
				case INT:
					return val.equals(Integer.parseInt(literal));
				case LONG:
					return val.equals(Long.parseLong(literal));
				case STRING:
					return val.equals(literal);
				default:
					break;
			
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		return false;
	}
	public static boolean isCompatible(Type source, Type target) {
		return isPrimitiveType(source) && isPrimitiveType(target);
	}
	public static boolean isPrimitiveType(Type t) {
		return t == Type.STRING || t == Type.BOOLEAN || t == Type.BYTES || t == Type.DOUBLE
				|| t == Type.FLOAT || t == Type.INT || t == Type.LONG;
		
	}
	/**
	 * 
	 * @param from
	 * @param to
	 * @param val
	 * @return
	 */
	public static Object resolve(Schema.Type from, Schema.Type to, Object val) {
		if(from == to)
			return val;
		if(val != null && isPrimitiveType(from)) {
			String valStr = val.toString();
			try {
				switch (to) {
				    case STRING:  return valStr;
				    case BYTES:   return valStr.getBytes(StandardCharsets.ISO_8859_1);
				    case INT:     return Integer.parseInt(valStr);
				    case LONG:    return Long.parseLong(valStr);
				    case FLOAT:   return Float.parseFloat(valStr);
				    case DOUBLE:  return Double.parseDouble(valStr);
				    case BOOLEAN: return Boolean.parseBoolean(valStr);
				    case NULL:    return null;
				    default:
				    	throw new MappingException("type handling supported for primitive types only");
				}
			} catch (NumberFormatException e) {
				throw new MappingException("type conversion failed - "+e.getMessage());
			}
		}
		return val;
	}
}
