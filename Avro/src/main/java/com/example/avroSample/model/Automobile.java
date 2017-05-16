/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.example.avroSample.model;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class Automobile extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Automobile\",\"namespace\":\"com.example.avroSample.model\",\"fields\":[{\"name\":\"modelName\",\"type\":\"string\"},{\"name\":\"make\",\"type\":\"string\"},{\"name\":\"modelYear\",\"type\":\"int\"},{\"name\":\"passengerCapacity\",\"type\":\"int\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.CharSequence modelName;
  @Deprecated public java.lang.CharSequence make;
  @Deprecated public int modelYear;
  @Deprecated public int passengerCapacity;

  /**
   * Default constructor.
   */
  public Automobile() {}

  /**
   * All-args constructor.
   */
  public Automobile(java.lang.CharSequence modelName, java.lang.CharSequence make, java.lang.Integer modelYear, java.lang.Integer passengerCapacity) {
    this.modelName = modelName;
    this.make = make;
    this.modelYear = modelYear;
    this.passengerCapacity = passengerCapacity;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return modelName;
    case 1: return make;
    case 2: return modelYear;
    case 3: return passengerCapacity;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: modelName = (java.lang.CharSequence)value$; break;
    case 1: make = (java.lang.CharSequence)value$; break;
    case 2: modelYear = (java.lang.Integer)value$; break;
    case 3: passengerCapacity = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'modelName' field.
   */
  public java.lang.CharSequence getModelName() {
    return modelName;
  }

  /**
   * Sets the value of the 'modelName' field.
   * @param value the value to set.
   */
  public void setModelName(java.lang.CharSequence value) {
    this.modelName = value;
  }

  /**
   * Gets the value of the 'make' field.
   */
  public java.lang.CharSequence getMake() {
    return make;
  }

  /**
   * Sets the value of the 'make' field.
   * @param value the value to set.
   */
  public void setMake(java.lang.CharSequence value) {
    this.make = value;
  }

  /**
   * Gets the value of the 'modelYear' field.
   */
  public java.lang.Integer getModelYear() {
    return modelYear;
  }

  /**
   * Sets the value of the 'modelYear' field.
   * @param value the value to set.
   */
  public void setModelYear(java.lang.Integer value) {
    this.modelYear = value;
  }

  /**
   * Gets the value of the 'passengerCapacity' field.
   */
  public java.lang.Integer getPassengerCapacity() {
    return passengerCapacity;
  }

  /**
   * Sets the value of the 'passengerCapacity' field.
   * @param value the value to set.
   */
  public void setPassengerCapacity(java.lang.Integer value) {
    this.passengerCapacity = value;
  }

  /** Creates a new Automobile RecordBuilder */
  public static com.example.avroSample.model.Automobile.Builder newBuilder() {
    return new com.example.avroSample.model.Automobile.Builder();
  }
  
  /** Creates a new Automobile RecordBuilder by copying an existing Builder */
  public static com.example.avroSample.model.Automobile.Builder newBuilder(com.example.avroSample.model.Automobile.Builder other) {
    return new com.example.avroSample.model.Automobile.Builder(other);
  }
  
  /** Creates a new Automobile RecordBuilder by copying an existing Automobile instance */
  public static com.example.avroSample.model.Automobile.Builder newBuilder(com.example.avroSample.model.Automobile other) {
    return new com.example.avroSample.model.Automobile.Builder(other);
  }
  
  /**
   * RecordBuilder for Automobile instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Automobile>
    implements org.apache.avro.data.RecordBuilder<Automobile> {

    private java.lang.CharSequence modelName;
    private java.lang.CharSequence make;
    private int modelYear;
    private int passengerCapacity;

    /** Creates a new Builder */
    private Builder() {
      super(com.example.avroSample.model.Automobile.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.example.avroSample.model.Automobile.Builder other) {
      super(other);
    }
    
    /** Creates a Builder by copying an existing Automobile instance */
    private Builder(com.example.avroSample.model.Automobile other) {
            super(com.example.avroSample.model.Automobile.SCHEMA$);
      if (isValidValue(fields()[0], other.modelName)) {
        this.modelName = data().deepCopy(fields()[0].schema(), other.modelName);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.make)) {
        this.make = data().deepCopy(fields()[1].schema(), other.make);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.modelYear)) {
        this.modelYear = data().deepCopy(fields()[2].schema(), other.modelYear);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.passengerCapacity)) {
        this.passengerCapacity = data().deepCopy(fields()[3].schema(), other.passengerCapacity);
        fieldSetFlags()[3] = true;
      }
    }

    /** Gets the value of the 'modelName' field */
    public java.lang.CharSequence getModelName() {
      return modelName;
    }
    
    /** Sets the value of the 'modelName' field */
    public com.example.avroSample.model.Automobile.Builder setModelName(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.modelName = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'modelName' field has been set */
    public boolean hasModelName() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'modelName' field */
    public com.example.avroSample.model.Automobile.Builder clearModelName() {
      modelName = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'make' field */
    public java.lang.CharSequence getMake() {
      return make;
    }
    
    /** Sets the value of the 'make' field */
    public com.example.avroSample.model.Automobile.Builder setMake(java.lang.CharSequence value) {
      validate(fields()[1], value);
      this.make = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'make' field has been set */
    public boolean hasMake() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'make' field */
    public com.example.avroSample.model.Automobile.Builder clearMake() {
      make = null;
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'modelYear' field */
    public java.lang.Integer getModelYear() {
      return modelYear;
    }
    
    /** Sets the value of the 'modelYear' field */
    public com.example.avroSample.model.Automobile.Builder setModelYear(int value) {
      validate(fields()[2], value);
      this.modelYear = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'modelYear' field has been set */
    public boolean hasModelYear() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'modelYear' field */
    public com.example.avroSample.model.Automobile.Builder clearModelYear() {
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'passengerCapacity' field */
    public java.lang.Integer getPassengerCapacity() {
      return passengerCapacity;
    }
    
    /** Sets the value of the 'passengerCapacity' field */
    public com.example.avroSample.model.Automobile.Builder setPassengerCapacity(int value) {
      validate(fields()[3], value);
      this.passengerCapacity = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'passengerCapacity' field has been set */
    public boolean hasPassengerCapacity() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'passengerCapacity' field */
    public com.example.avroSample.model.Automobile.Builder clearPassengerCapacity() {
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    public Automobile build() {
      try {
        Automobile record = new Automobile();
        record.modelName = fieldSetFlags()[0] ? this.modelName : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.make = fieldSetFlags()[1] ? this.make : (java.lang.CharSequence) defaultValue(fields()[1]);
        record.modelYear = fieldSetFlags()[2] ? this.modelYear : (java.lang.Integer) defaultValue(fields()[2]);
        record.passengerCapacity = fieldSetFlags()[3] ? this.passengerCapacity : (java.lang.Integer) defaultValue(fields()[3]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
