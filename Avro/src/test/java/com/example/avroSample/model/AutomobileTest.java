package com.example.avroSample.model;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

public class AutomobileTest {

    private static final Logger LOGGER = LogManager.getLogger(AutomobileTest.class);
    private static final File outputFile = new File("target/avro/autos.avro");

    @BeforeClass
    public static void setup() {

        LOGGER.info("Starting Automobile serializer example...");
        
        if (outputFile.exists()) {
            outputFile.delete();
        }
        
        new File("target/avro").mkdir();

        // Setting values to object
        Automobile auto = Automobile.newBuilder()
                .setMake("GAZ")
                .setModelName("Volga")
                .setModelYear(1988)
                .setPassengerCapacity(5)
                    .build();

        DatumWriter<Automobile> datumWriter = new SpecificDatumWriter<Automobile>(
                Automobile.class);
        // Special object to write data to file
        DataFileWriter<Automobile> fileWriter = new DataFileWriter<Automobile>(
                datumWriter);
        try {
            fileWriter.create(auto.getSchema(), outputFile);
            fileWriter.append(auto); // appending object to file
            fileWriter.close();
        } catch (IOException e) {
            LOGGER.error("Error while trying to write the object to file <"
                    + outputFile.getAbsolutePath() + ">.", e);
        }

        LOGGER.info("Finished running Automobile serializer example.");

    }

    @Test
    public void testDeserialize() {
        
        DatumReader<Automobile> datumReader = new SpecificDatumReader<Automobile>(
                Automobile.class);
        try {
            // Special object to read data from file (works like Iterator)
            DataFileReader<Automobile> fileReader = new DataFileReader<Automobile>(
                    outputFile, datumReader);
            Automobile auto = null;
            
            if (fileReader.hasNext()) {
                auto = fileReader.next(auto);
            }
            
            assertEquals("GAZ", auto.getMake().toString());
            assertEquals("Volga", auto.getModelName().toString());
            assertEquals(Integer.valueOf(1988), auto.getModelYear());
            
            
        } catch (IOException e) {
            LOGGER.error("Error while trying to read the object from file <"
                    + outputFile.getAbsolutePath() + ">.", e);
        }
    }

}
