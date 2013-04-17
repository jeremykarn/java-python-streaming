import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.avro.Schema;
import org.apache.avro.file.DirectDataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

public class AvroTest {
    private BlockingQueue<Object> outputQueue;

    public AvroTest() throws Exception {
        outputQueue = new ArrayBlockingQueue<Object>(2);

        ProcessBuilder processBuilder = new ProcessBuilder("python", "stream_test.py");
        Process process = processBuilder.start();

        ExecutorService pool = Executors.newFixedThreadPool(3);
        pool.submit(new ErrorReader(process.getErrorStream()));
        pool.submit(new AvroReader(process.getInputStream()));
        pool.submit(new AvroWriter(process.getOutputStream()));
    }

    public class AvroWriter implements Callable<Boolean> {
        private ReflectDatumWriter< GenericRecord > reflectDatumWriter = null;
        private DataFileWriter< GenericRecord > writer = null;
        private Schema toPythonSchema = null;

        public AvroWriter(OutputStream outputStream) throws Exception {
            String toPythonSchemaStr = "{\"namespace\": \"example.avro\"," +
                    "\"type\": \"record\"," +
                    "\"name\": \"UDF\"," +
                    "\"fields\": [ {\"name\": \"DBL\", \"type\": \"double\"}," +
                                  "{\"name\": \"STR\", \"type\": \"string\"}]}";

            toPythonSchema = Schema.parse(toPythonSchemaStr);
            reflectDatumWriter = new ReflectDatumWriter< GenericRecord >(toPythonSchema);
            writer = (new DataFileWriter< GenericRecord >(reflectDatumWriter))
                     .create(toPythonSchema, new BufferedOutputStream(outputStream));
        }

        @Override
        public Boolean call() throws Exception {
            write(1.0, "aardvark");
            System.out.println("Recieved from Python: " + outputQueue.take());
            write(2.0, "beryllium");
            System.out.println("Recieved from Python: " + outputQueue.take());
            write(3.0, "cerulean");
            System.out.println("Recieved from Python: " + outputQueue.take());
            return true;
        }
        
        public void write(Double d, String s) throws IOException {
            GenericRecord output = new GenericData.Record(toPythonSchema);
            output.put("DBL", d);
            output.put("STR", s);
            System.out.println("Sent to python: " + output.toString());
            writer.append(output);
            writer.flush();
        }
    }

    
    public class AvroReader implements Callable<Boolean> {
        private ReflectDatumReader< GenericRecord > reflectDatumReader = null;
        private DirectDataFileStream< GenericRecord > reader = null;
        private InputStream inputStream;

        public AvroReader(InputStream inputStream) throws Exception {
            this.inputStream = inputStream;
            reflectDatumReader = new ReflectDatumReader< GenericRecord >();
        }
        
        @Override
        public Boolean call() throws Exception {
            try {
                // reader reads data as part of its initialization process
                // so we need to instantiate it here instead of in the AvroReader constructor
                reader = new DirectDataFileStream< GenericRecord >(inputStream, reflectDatumReader);

                GenericRecord record;
                while (reader.hasNext()) {
                    record = reader.next();

                    // DEBUG: verify that proper datatypes are maintained
                    Double d = (Double) record.get("DBL");
                    String s = (String) record.get("STR");
                    
                    outputQueue.put(record);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            return true;
        }
    }
    
    public class ErrorReader implements Callable<Boolean> {
        private BufferedReader reader;

        public ErrorReader(InputStream inputStream) throws Exception {
            reader = new BufferedReader(new InputStreamReader(inputStream));
        }
        
        @Override
        public Boolean call() throws Exception {
            String errInput;
            while ((errInput = reader.readLine()) != null) {
                System.out.println("Error: " + errInput);
            }
            return true;
        }
    }
    
    public static void main(String[] args) {
        try {
            AvroTest at = new AvroTest();
        } catch (Exception e) {
            System.out.println("Exception: " + e.toString());
            e.printStackTrace();
        }
    }
}
