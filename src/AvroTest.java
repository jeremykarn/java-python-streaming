import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
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
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.DirectDataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.streaming.DefaultInputHandler;
import org.apache.pig.impl.streaming.DefaultOutputHandler;
import org.apache.pig.impl.streaming.InputHandler;
import org.apache.pig.impl.streaming.OutputHandler;
import org.apache.pig.impl.util.Utils;

public class AvroTest {
    private BlockingQueue<Object> outputQueue;

    public enum PythonType {
        AVRO,
        FASTAVRO,
        OLD;
    };
    public PythonType PYTHON_TYPE = PythonType.OLD;
    
    public File logFile = new File(PYTHON_TYPE.toString() + "_java.output");
    BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(logFile));
    
    public void log(String m) {
        try {
            bos.write(m.getBytes());
            bos.write("\n".getBytes());
        } catch (Exception e) {
            //Why won't you be a team player?
        }
         
    }
    
    public AvroTest() throws Exception {
        outputQueue = new ArrayBlockingQueue<Object>(2);
        
        ProcessBuilder  processBuilder;
        switch (PYTHON_TYPE) {
        case AVRO:
            processBuilder = new ProcessBuilder("python", "stream_test.py");
            break;
        case FASTAVRO:
            processBuilder = new ProcessBuilder("python", "fast_stream_test.py");
            break;
        case OLD:
            processBuilder = new ProcessBuilder("python", "controller.py");
            break;
        default:
            throw new RuntimeException("UNKNOWN TYPE");
        }

        Process process = processBuilder.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new ProcessKiller(process) ) );

        ExecutorService pool = Executors.newFixedThreadPool(3);
        pool.submit(new ErrorReader(process.getErrorStream()));
        pool.submit(new Reader(process.getInputStream()));
        Future writerF = pool.submit(new Writer(process.getOutputStream()));
        while (!writerF.isDone()) {}
    }

    public class Writer implements Callable<Boolean> {
        private GenericDatumWriter< GenericRecord > reflectDatumWriter = null;
        private DataFileWriter< GenericRecord > writer = null;
        private Schema toPythonSchema = null;
        private InputHandler inputHandler;
        private OutputStream outputStream;

        public Writer(OutputStream outputStreamm) throws Exception {
            switch (PYTHON_TYPE) {
            case AVRO:
            case FASTAVRO:
                //toPythonSchema = getSimplePythonSchema();
                //toPythonSchema = getMapPythonSchema();
                toPythonSchema = getDetailedMapPythonSchema();
                reflectDatumWriter = new GenericDatumWriter< GenericRecord >(toPythonSchema);
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter< GenericRecord >(reflectDatumWriter);
                writer = dataFileWriter.create(toPythonSchema, new BufferedOutputStream(outputStreamm));
                break;
            case OLD:
                inputHandler = new DefaultInputHandler();
                outputStream = new DataOutputStream(new BufferedOutputStream(outputStreamm));
                inputHandler.bindTo(outputStream);
            }
        }

        private Schema getSimplePythonSchema() {
            String toPythonSchemaStr = "{\"namespace\": \"example.avro\"," +
                    "\"type\": \"record\"," +
                    "\"name\": \"Udf\"," +
                    "\"fields\": [ " +
                                  "{\"name\": \"A1\", \"type\": \"double\"}," +
                                  "{\"name\": \"A2\", \"type\": \"string\"}," +
                                  "{\"name\": \"A3\", \"type\": \"double\"}," +
                                  "{\"name\": \"A4\", \"type\": \"string\"}," +
                                  "{\"name\": \"A5\", \"type\": \"double\"}," +
                                  "{\"name\": \"A6\", \"type\": \"string\"}," +
                                  "{\"name\": \"A7\", \"type\": \"double\"}," +
                                  "{\"name\": \"A8\", \"type\": \"string\"}," +
                                  "{\"name\": \"A9\", \"type\": \"double\"}," +
                                  "{\"name\": \"A10\", \"type\": \"string\"}" +
                                  "]}";
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

        private Schema getDetailedMapPythonSchema() {
            String toPythonSchemaStr = 
                    "{ \"type\": \"record\"," +
                    "\"name\": \"pseudojson\"," +
                    "\"fields\": [" +
                      "{" +
                          "\"name\": \"v\"," +
                          "\"type\":{\"type\": \"map\", \"values\":\"string\"}" +
                      "}" +
                      " ]" +
                   "}";
            return org.apache.avro.Schema.parse(toPythonSchemaStr);
        }
        
        @Override
        public Boolean call() throws Exception {
            //return simpleCall();
            //return mapCall();
            return detailedMapCall();
        }
        
        public Map<String,Object>[] getMaps() {
            Map<String, Object> m1 = new HashMap<String, Object>();
            m1.put("F1", 12341235123412.0D);
            m1.put("F2", 12341235123412.0D);
            m1.put("F3", 12341235123412.0D);
            m1.put("F4", 12341235123412.0D);
            m1.put("F5", 12341235123412.0D);
            m1.put("F6", 12341235123412.0D);
            m1.put("F7", 12341235123412.0D);
            m1.put("F8", 12341235123412.0D);
            m1.put("F9", 12341235123412.0D);
                        
            Map<String, Object> m2 = new HashMap<String, Object>();
            m2.put("F1", new Long(3));
            m2.put("F2", new Double(4.0));
            m2.put("F3", m1);

            
            Map<String, Object> m3 = new HashMap<String, Object>();
            m3.put("F1", new Long(5));
            m3.put("F2", true);
            m3.put("F4", m2);
            
            Map<String, Object> m4 = new HashMap<String, Object>();
            m4.put("F1", "Some really big long string. Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.");
            m4.put("F2", "Some really big long string. Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.");
            m4.put("F3", "Some really big long string. Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.");
            m4.put("F4", "Some really big long string. Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.");
            m4.put("F5", "Some really big long string. Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.");
            m4.put("F6", "Some really big long string. Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.");
            m4.put("F7", "Some really big long string. Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.");
            m4.put("F8", "Some really big long string. Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.");
            m4.put("F9", "Some really big long string. Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.Some really big long string.");
            //Map[] mapArray = new Map[] {m1,m2,m3};
            Map<String,Object>[] mapArray = new Map[] {m4};
            return mapArray;
        }
        
        public Boolean detailedMapCall() throws Exception {
            Map[] mapArray = getMaps();
            for (int i = 0; i < 10000; i++) {
                writeDetailedMap(mapArray[i%mapArray.length]);
                log("Out: " + outputQueue.take());
            }
            log("Done writing maps");

            return true;
        }

        public Boolean mapCall() throws Exception {
            Map[] mapArray = getMaps();
            for (int i = 0; i < 10000; i++) {
                writeMap(mapArray[i%mapArray.length]);
                log("Out: " + outputQueue.take());
            }
            log("Done writing maps");

            return true;
        }
        
        public Boolean simpleCall() throws Exception {
            for (int i = 0; i < 10000; i++) {
                writeSimple((i + 0.5D) * 12345, "" + i + "123as;dfkas;dkfja;skdfj;alsdfja;lsdfja;sldfjjka;sdfasdkjhfalsdhflasj lfahsl hflsajdhf aslkhf alslaskjdf laskdf laksdhf alksdfl kasjfl aksfhlkajdfl akj sdflkjashd flkajsdflaksjdf lksahf klsajflksalkdfsklashdf alskjfh alksjdfh alkjsdhf laksjdhf laksjhdf alksdhf alksjdfh laksjhdf laksdhjf lskhjdf ");
                log("Out: " + outputQueue.take());
            }
            return true;
        }
        
        public void writeSimple(Double a, String b) throws IOException {
            switch (PYTHON_TYPE) {
            case AVRO:
            case FASTAVRO:
                GenericRecord output = new GenericData.Record(toPythonSchema);
                output.put("A1", a);
                output.put("A2", b);
                output.put("A3", a);
                output.put("A4", b);
                output.put("A5", a);
                output.put("A6", b);
                output.put("A7", a);
                output.put("A8", b);
                output.put("A9", a);
                output.put("A10", b);
                writer.append(output);
                writer.flush();
                break;
            case OLD:
                Tuple t = DefaultTupleFactory.getInstance().newTuple(10);
                t.set(0, a);
                t.set(1, b);
                t.set(2, a);
                t.set(3, b);
                t.set(4, a);
                t.set(5, b);
                t.set(6, a);
                t.set(7, b);
                t.set(8, a);
                t.set(9, b);
                inputHandler.putNext(t, true, true, null);
                outputStream.flush();
                break;
            }
        }
        
        public void writeMap(Map<String, Object> m) throws Exception {
            switch (PYTHON_TYPE) {
            case AVRO:
            case FASTAVRO:
                writeAvroMap(m);
                break;
            case OLD:
                writeOldMap(m);
                break;
            }
        }
        
        public void writeDetailedMap(Map<String, Object> m) throws Exception {
            switch (PYTHON_TYPE) {
            case AVRO:
            case FASTAVRO:
                writeAvroDetailedMap(m);
                break;
            case OLD:
                writeOldMap(m);
                break;
            }
        }
        
        public void writeOldMap(Map<String, Object> m) throws Exception {
            Tuple t = DefaultTupleFactory.getInstance().newTuple(1);
            t.set(0, m);
            inputHandler.putNext(t, true, true, new HashMap());
            outputStream.flush();
        }
        
        public void writeAvroMap(Map<String, Object> m) throws IOException {
            Map<String,Object> avroMap = createAvroMap(m);
            
            GenericRecord output = new GenericData.Record(toPythonSchema);
            output.put("v", avroMap);
            
            writer.append(output);
            writer.flush();
        }
        
        public void writeAvroDetailedMap(Map<String, Object> m) throws IOException {
            GenericRecord output = new GenericData.Record(toPythonSchema);
            output.put("v", m);
            
            writer.append(output);
            writer.flush();
        }
        
        //Leaves original map untouched.
        public Map<String, Object> createAvroMap(Map<String, Object> m) {
            Map<String, Object> avroMap = new HashMap<String,Object>(m.size());

            for (Entry<String,Object> e : m.entrySet()) {
                Object val = e.getValue();
                if (val instanceof Map) {
                    val = createAvroMap((Map)val);
                }

                GenericRecord avroVal = new GenericData.Record(toPythonSchema);
                avroVal.put("v", val);

                avroMap.put(e.getKey(), avroVal);
            }
            return avroMap;
        }
        
        /*
         * This was used to test avro where I avoided creating a new map
         * but instead destroyed the passed in one.
         * 
        public Map<String, Object> createAvroMap(Map<String, Object> m) {
            for (Entry<String,Object> e : m.entrySet()) {
                Object val = e.getValue();
                if (val instanceof Map) {
                    val = createAvroMap((Map)val);
                }

                GenericRecord avroVal = new GenericData.Record(toPythonSchema);
                avroVal.put("v", val);
                e.setValue(avroVal);
            }
            return m;
        }
        */
    }

    
    public class Reader implements Callable<Boolean> {
        private GenericDatumReader< GenericRecord > reflectDatumReader = null;
        private DirectDataFileStream< GenericRecord > reader = null;
        private InputStream inputStream;
        
        //For Old
        private OutputHandler outputHandler;

        public Reader(InputStream inputStream) throws Exception {
            this.inputStream = inputStream;
            reflectDatumReader = new GenericDatumReader< GenericRecord >();
            
            //For Old
            if (PYTHON_TYPE == PythonType.OLD) {
                outputHandler = new DefaultOutputHandler();
                outputHandler.bindTo("", new BufferedPositionedInputStream(inputStream), 0, Long.MAX_VALUE);
                //outputHandler.bindTo("", new BufferedInputStream(inputStream), 0, Long.MAX_VALUE);
            }
        }
        
        @Override
        public Boolean call() throws Exception {
            switch (PYTHON_TYPE) {
            case AVRO:
            case FASTAVRO:
                return callAvro();
            case OLD:
                return callOld();
            default:
                throw new RuntimeException("Unknown type");
            }
        }
        
        private Boolean callOld() throws Exception{
            //TODO: Need to make this part not hardcoded.
            FieldSchema fs = Utils.getSchemaFromString(("m:map[]")).getField(0);
            //FieldSchema fs = Utils.getSchemaFromString("t:(A1:double, A2:chararray, A3:double, A4:chararray, A5:double, A6:chararray, A7:double, A8:chararray, A9:double, A10:chararray)").getField(0);
            Object o = outputHandler.getNext(fs, null);
            while (o != null) {
                outputQueue.put(o);
                o = outputHandler.getNext(fs, null);
            }
            return true;
        }
        
        private Boolean callAvro() throws Exception{
            try {
                // reader reads data as part of its initialization process
                // so we need to instantiate it here instead of in the AvroReader constructor
                reader = new DirectDataFileStream< GenericRecord >(inputStream, reflectDatumReader);

                GenericRecord record;
                while (reader.hasNext()) {
                    record = reader.next();
                    
                    //Map<String,Object> m = (Map) record.get("v");
                    Object m = record;
                    
                    //outputQueue.put(createFromAvroMap(m));
                    outputQueue.put(m);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            return true;
        }
        
        
        
        private Map<String, Object> createFromAvroMap(Map<String, Object> m) {
            Map<String, Object> newM = new HashMap<String,Object>(m.size());
            for (Map.Entry<String, Object> e : m.entrySet()) {
                Object v = ((GenericData.Record)e.getValue()).get("v");
                if (v instanceof Map) {
                    v = createFromAvroMap((Map)v);
                }
                newM.put(e.getKey(), v);
            }
            return newM;
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
    
    public class ProcessKiller implements Runnable {
        private Process process;
        public ProcessKiller(Process process) {
            this.process = process;
        }
        public void run() {
            process.destroy();
        }
    }
    
    public static void main(String[] args) {
        try {
            //Add the sleep in if you want to do Java profiling.  It gives you enough time
            //after starting the process to use VisualVM to start profiling before anything
            //important happens.
            //Thread.sleep(10*1000);
            AvroTest at = new AvroTest();
            System.out.println("Done!");
            Thread.sleep(3*1000);
            System.exit(0);
        } catch (Exception e) {
            System.out.println("Exception: " + e.toString());
            e.printStackTrace();
        }
    }
}
