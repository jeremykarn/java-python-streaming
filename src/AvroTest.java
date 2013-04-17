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
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

public class AvroTest {
    private BlockingQueue<Object> outputQueue;


    public AvroTest() throws Exception {
        outputQueue = new ArrayBlockingQueue<Object>(2);
        ProcessBuilder  processBuilder = new ProcessBuilder("python", "stream_test.py");

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
                    "\"name\": \"Udf\"," +
                    "\"fields\": [ {\"name\": \"A\", \"type\": \"double\"}," +
                                  "{\"name\": \"B\", \"type\": \"string\"}]}";
            toPythonSchema = org.apache.avro.Schema.parse(toPythonSchemaStr);
            reflectDatumWriter = new ReflectDatumWriter< GenericRecord >(toPythonSchema);
            writer = new DataFileWriter< GenericRecord >(reflectDatumWriter).create(toPythonSchema, new BufferedOutputStream(outputStream));
        }

        @Override
        public Boolean call() throws Exception {
            write(1.0, "One");
            System.out.println("Out: " + outputQueue.take());
            write(2.0, "Two");
            System.out.println("Out: " + outputQueue.take());
            write(3.0, "Three");
            System.out.println("Out: " + outputQueue.take());
            return true;
        }
        
        public void write(Double a, String b) throws IOException {
            GenericRecord output = new GenericData.Record(toPythonSchema);
            output.put("A", a);
            output.put("B", b);

            writer.append(output);
            writer.flush();
        }
    }

    
    public class AvroReader implements Callable<Boolean> {
        private ReflectDatumReader< GenericRecord > reflectDatumReader = null;
        private DataFileStream< GenericRecord > avroReader = null;
        private BufferedReader reader;

        
        public AvroReader(InputStream inputStream) throws Exception {
            reader = new BufferedReader(new InputStreamReader(inputStream));
            /*
            reflectDatumReader = new ReflectDatumReader< GenericRecord >();
            avroReader = new DataFileStream<GenericRecord>(inputStream, reflectDatumReader);
            */
        }
        
        @Override
        public Boolean call() throws Exception {
            //GenericRecord gr = avroReader.next();
            String input;
            while ((input = reader.readLine()) != null) {
                System.out.println("Val: " + input);
                outputQueue.put(input);
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
