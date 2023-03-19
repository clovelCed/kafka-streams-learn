/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package fr.viteducode.kafkaclient.producer.simple.data;

import org.apache.avro.generic.GenericArray;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.Utf8;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;

@org.apache.avro.specific.AvroGenerated
public class CompanieKey extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -7275030704787102850L;


  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"CompanieKey\",\"namespace\":\"fr.viteducode.kafkaclient.producer.simple.data\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static final SpecificData MODEL$ = new SpecificData();

  private static final BinaryMessageEncoder<CompanieKey> ENCODER =
      new BinaryMessageEncoder<CompanieKey>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<CompanieKey> DECODER =
      new BinaryMessageDecoder<CompanieKey>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<CompanieKey> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<CompanieKey> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<CompanieKey> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<CompanieKey>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this CompanieKey to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a CompanieKey from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a CompanieKey instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static CompanieKey fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

  private int id;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public CompanieKey() {}

  /**
   * All-args constructor.
   * @param id The new value for id
   */
  public CompanieKey(java.lang.Integer id) {
    this.id = id;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return id;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: id = (java.lang.Integer)value$; break;
    default: throw new IndexOutOfBoundsException("Invalid index: " + field$);
    }
  }

  /**
   * Gets the value of the 'id' field.
   * @return The value of the 'id' field.
   */
  public int getId() {
    return id;
  }


  /**
   * Sets the value of the 'id' field.
   * @param value the value to set.
   */
  public void setId(int value) {
    this.id = value;
  }

  /**
   * Creates a new CompanieKey RecordBuilder.
   * @return A new CompanieKey RecordBuilder
   */
  public static fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder newBuilder() {
    return new fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder();
  }

  /**
   * Creates a new CompanieKey RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new CompanieKey RecordBuilder
   */
  public static fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder newBuilder(fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder other) {
    if (other == null) {
      return new fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder();
    } else {
      return new fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder(other);
    }
  }

  /**
   * Creates a new CompanieKey RecordBuilder by copying an existing CompanieKey instance.
   * @param other The existing instance to copy.
   * @return A new CompanieKey RecordBuilder
   */
  public static fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder newBuilder(fr.viteducode.kafkaclient.producer.simple.data.CompanieKey other) {
    if (other == null) {
      return new fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder();
    } else {
      return new fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder(other);
    }
  }

  /**
   * RecordBuilder for CompanieKey instances.
   */
  @org.apache.avro.specific.AvroGenerated
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<CompanieKey>
    implements org.apache.avro.data.RecordBuilder<CompanieKey> {

    private int id;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$, MODEL$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
    }

    /**
     * Creates a Builder by copying an existing CompanieKey instance
     * @param other The existing instance to copy.
     */
    private Builder(fr.viteducode.kafkaclient.producer.simple.data.CompanieKey other) {
      super(SCHEMA$, MODEL$);
      if (isValidValue(fields()[0], other.id)) {
        this.id = data().deepCopy(fields()[0].schema(), other.id);
        fieldSetFlags()[0] = true;
      }
    }

    /**
      * Gets the value of the 'id' field.
      * @return The value.
      */
    public int getId() {
      return id;
    }


    /**
      * Sets the value of the 'id' field.
      * @param value The value of 'id'.
      * @return This builder.
      */
    public fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder setId(int value) {
      validate(fields()[0], value);
      this.id = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'id' field has been set.
      * @return True if the 'id' field has been set, false otherwise.
      */
    public boolean hasId() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'id' field.
      * @return This builder.
      */
    public fr.viteducode.kafkaclient.producer.simple.data.CompanieKey.Builder clearId() {
      fieldSetFlags()[0] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompanieKey build() {
      try {
        CompanieKey record = new CompanieKey();
        record.id = fieldSetFlags()[0] ? this.id : (java.lang.Integer) defaultValue(fields()[0]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<CompanieKey>
    WRITER$ = (org.apache.avro.io.DatumWriter<CompanieKey>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<CompanieKey>
    READER$ = (org.apache.avro.io.DatumReader<CompanieKey>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

  @Override protected boolean hasCustomCoders() { return true; }

  @Override public void customEncode(org.apache.avro.io.Encoder out)
    throws java.io.IOException
  {
    out.writeInt(this.id);

  }

  @Override public void customDecode(org.apache.avro.io.ResolvingDecoder in)
    throws java.io.IOException
  {
    org.apache.avro.Schema.Field[] fieldOrder = in.readFieldOrderIfDiff();
    if (fieldOrder == null) {
      this.id = in.readInt();

    } else {
      for (int i = 0; i < 1; i++) {
        switch (fieldOrder[i].pos()) {
        case 0:
          this.id = in.readInt();
          break;

        default:
          throw new java.io.IOException("Corrupt ResolvingDecoder.");
        }
      }
    }
  }
}










