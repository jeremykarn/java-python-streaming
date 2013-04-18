import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.streaming.DefaultInputHandler;
import org.apache.pig.impl.streaming.InputHandler;

public class AvroTest {
    private BlockingQueue<Object> outputQueue;

    private boolean USE_AVRO = true;
    
    
    public AvroTest() throws Exception {
        outputQueue = new ArrayBlockingQueue<Object>(2);
        
        ProcessBuilder  processBuilder;
        if (USE_AVRO) {
            processBuilder = new ProcessBuilder("python", "stream_test.py");
        } else {
            processBuilder = new ProcessBuilder("python", "controller.py");
        }

        Process process = processBuilder.start();

        ExecutorService pool = Executors.newFixedThreadPool(3);
        pool.submit(new ErrorReader(process.getErrorStream()));
        pool.submit(new Reader(process.getInputStream()));
        pool.submit(new Writer(process.getOutputStream()));
    }

    public class Writer implements Callable<Boolean> {
        private ReflectDatumWriter< GenericRecord > reflectDatumWriter = null;
        private DataFileWriter< GenericRecord > writer = null;
        private Schema toPythonSchema = null;
        private InputHandler inputHandler;
        private OutputStream outputStream;

        public Writer(OutputStream outputStreamm) throws Exception {
            if (USE_AVRO) {
                toPythonSchema = getMapPythonSchema();
                //toPythonSchema = getSimplePythonSchema();
                reflectDatumWriter = new ReflectDatumWriter< GenericRecord >(toPythonSchema);
                writer = new DataFileWriter< GenericRecord >(reflectDatumWriter).create(toPythonSchema, new BufferedOutputStream(outputStreamm));
            } else {
                inputHandler = new DefaultInputHandler();
                outputStream = new DataOutputStream(new BufferedOutputStream(outputStreamm));
                inputHandler.bindTo(outputStream);
            }
        }

        private Schema getSimplePythonSchema() {
            String toPythonSchemaStr = "{\"namespace\": \"example.avro\"," +
                    "\"type\": \"record\"," +
                    "\"name\": \"Udf\"," +
                    "\"fields\": [ {\"name\": \"A\", \"type\": \"double\"}," +
                                  "{\"name\": \"B\", \"type\": \"string\"}]}";
            return org.apache.avro.Schema.parse(toPythonSchemaStr);
        }
        
        private Schema getMapPythonSchema() {
            String toPythonSchemaStr = 
                "{ \"type\": \"record\"," +
                  "\"name\": \"pseudojson\"," +
                  "\"fields\": [ {" +
                        "\"name\": \"v\"," +
                        "\"type\": [" +
                            "\"long\"," +
                            "\"double\"," +
                            "\"string\"," +
                            "\"boolean\"," +
                            "\"null\"," +
                            "{\"type\": \"array\", \"items\": \"pseudojson\"}," +
                            "{\"type\": \"map\", \"values\": \"pseudojson\"}" +
                        "]" +
                    "} ]" +
                 "}";
            return org.apache.avro.Schema.parse(toPythonSchemaStr);
        }

        @Override
        public Boolean call() throws Exception {
            //return simpleCall();
            return mapCall();
        }
        
        public Boolean mapCall() throws Exception {
            Map<String, Object> m1 = new HashMap<String, Object>();
            m1.put("F1", new Long(1));
            m1.put("F2", "2");
            
            Map<String, Object> m2 = new HashMap<String, Object>();
            m2.put("F1", new Long(3));
            m2.put("F2", new Double(4.0));
            m2.put("F3", m1);
            
            Map<String, Object> m3 = new HashMap<String, Object>();
            m3.put("F1", new Long(5));
            m3.put("F2", true);
            m3.put("F4", m2);
            
            System.out.println("Before write out m1");
            writeMap(m1);
            System.out.println("After write out m1");
            System.out.println("Out: " + outputQueue.take());
            writeMap(m2);
            System.out.println("Out: " + outputQueue.take());
            writeMap(m3);
            System.out.println("Out: " + outputQueue.take());
            return true;
        }
        
        public Boolean simpleCall() throws Exception {
            writeSimple(1.0, "One");
            System.out.println("Out: " + outputQueue.take());
            writeSimple(2.0, "Two");
            System.out.println("Out: " + outputQueue.take());
            writeSimple(3.0, "Three");
            System.out.println("Out: " + outputQueue.take());
            return true;
        }
        
        public void writeSimple(Double a, String b) throws IOException {
            GenericRecord output = new GenericData.Record(toPythonSchema);
            output.put("A", a);
            output.put("B", b);

            writer.append(output);
            writer.flush();
        }
        
        public void writeMap(Map<String, Object> m) throws Exception {
            if (USE_AVRO) {
                writeAvroMap(m);
            } else {
                writeOldMap(m);
            }
        }
        
        public void writeOldMap(Map<String, Object> m) throws Exception {
            Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
            t.set(0, m);
            inputHandler.putNext(t, true, true, new HashMap());
            outputStream.flush();
        }
        
        public void writeAvroMap(Map<String, Object> m) throws IOException {
            Map<String,Object> avroMap = mapConverter(m);
            
            GenericRecord output = new GenericData.Record(toPythonSchema);
            output.put("v", avroMap);
            
            System.out.println("Write out: " + output.toString());
            writer.append(output);
            writer.flush();
        }
        
        public Map<String, Object> mapConverter(Map<String, Object> m) {
            Map<String, Object> avroMap = new HashMap<String,Object>(m.size());

            for (Entry<String,Object> e : m.entrySet()) {
                Object val = e.getValue();
                if (val instanceof Map) {
                    val = mapConverter((Map)val);
                }

                GenericRecord avroVal = new GenericData.Record(toPythonSchema);
                avroVal.put("v", val);

                avroMap.put(e.getKey(), avroVal);
            }
            return avroMap;
        }
    }

    
    public class Reader implements Callable<Boolean> {
        private ReflectDatumReader< GenericRecord > reflectDatumReader = null;
        private DataFileStream< GenericRecord > avroReader = null;
        private BufferedReader reader;

        
        public Reader(InputStream inputStream) throws Exception {
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
