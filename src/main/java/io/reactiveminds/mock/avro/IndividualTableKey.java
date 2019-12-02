/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package io.reactiveminds.mock.avro;

import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.commons.compress.utils.Charsets;

@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class IndividualTableKey extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -2099717779562957577L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"IndividualTableKey\",\"namespace\":\"io.reactiveminds.mock.avro\",\"fields\":[{\"name\":\"individual_id\",\"type\":[\"null\",{\"type\":\"string\",\"avro.java.string\":\"String\"}],\"default\":null}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.String individual_id;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public IndividualTableKey() {}

  /**
   * All-args constructor.
   * @param individual_id The new value for individual_id
   */
  public IndividualTableKey(java.lang.String individual_id) {
    this.individual_id = individual_id;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return individual_id;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
	  Object value = value$;
		if(value$ instanceof Utf8) {
			value = new String(((Utf8) value$).getBytes(), Charsets.UTF_8);
		}
    switch (field$) {
    case 0: individual_id = (java.lang.String)value; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'individual_id' field.
   * @return The value of the 'individual_id' field.
   */
  public java.lang.String getIndividualId() {
    return individual_id;
  }

  /**
   * Sets the value of the 'individual_id' field.
   * @param value the value to set.
   */
  public void setIndividualId(java.lang.String value) {
    this.individual_id = value;
  }

  /**
   * Creates a new IndividualTableKey RecordBuilder.
   * @return A new IndividualTableKey RecordBuilder
   */
  public static io.reactiveminds.mock.avro.IndividualTableKey.Builder newBuilder() {
    return new io.reactiveminds.mock.avro.IndividualTableKey.Builder();
  }

  /**
   * Creates a new IndividualTableKey RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new IndividualTableKey RecordBuilder
   */
  public static io.reactiveminds.mock.avro.IndividualTableKey.Builder newBuilder(io.reactiveminds.mock.avro.IndividualTableKey.Builder other) {
    return new io.reactiveminds.mock.avro.IndividualTableKey.Builder(other);
  }

  /**
   * Creates a new IndividualTableKey RecordBuilder by copying an existing IndividualTableKey instance.
   * @param other The existing instance to copy.
   * @return A new IndividualTableKey RecordBuilder
   */
  public static io.reactiveminds.mock.avro.IndividualTableKey.Builder newBuilder(io.reactiveminds.mock.avro.IndividualTableKey other) {
    return new io.reactiveminds.mock.avro.IndividualTableKey.Builder(other);
  }

  /**
   * RecordBuilder for IndividualTableKey instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<IndividualTableKey>
    implements org.apache.avro.data.RecordBuilder<IndividualTableKey> {

    private java.lang.String individual_id;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(io.reactiveminds.mock.avro.IndividualTableKey.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.individual_id)) {
        this.individual_id = data().deepCopy(fields()[0].schema(), other.individual_id);
        fieldSetFlags()[0] = true;
      }
    }

    /**
     * Creates a Builder by copying an existing IndividualTableKey instance
     * @param other The existing instance to copy.
     */
    private Builder(io.reactiveminds.mock.avro.IndividualTableKey other) {
            super(SCHEMA$);
      if (isValidValue(fields()[0], other.individual_id)) {
        this.individual_id = data().deepCopy(fields()[0].schema(), other.individual_id);
        fieldSetFlags()[0] = true;
      }
    }

    /**
      * Gets the value of the 'individual_id' field.
      * @return The value.
      */
    public java.lang.String getIndividualId() {
      return individual_id;
    }

    /**
      * Sets the value of the 'individual_id' field.
      * @param value The value of 'individual_id'.
      * @return This builder.
      */
    public io.reactiveminds.mock.avro.IndividualTableKey.Builder setIndividualId(java.lang.String value) {
      validate(fields()[0], value);
      this.individual_id = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'individual_id' field has been set.
      * @return True if the 'individual_id' field has been set, false otherwise.
      */
    public boolean hasIndividualId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'individual_id' field.
      * @return This builder.
      */
    public io.reactiveminds.mock.avro.IndividualTableKey.Builder clearIndividualId() {
      individual_id = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    public IndividualTableKey build() {
      try {
        IndividualTableKey record = new IndividualTableKey();
        record.individual_id = fieldSetFlags()[0] ? this.individual_id : (java.lang.String) defaultValue(fields()[0]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  private static final org.apache.avro.io.DatumWriter
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader
    READER$ = new org.apache.avro.specific.SpecificDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}
