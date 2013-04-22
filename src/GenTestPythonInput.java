import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.pig.builtin.PigStreaming;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class GenTestPythonInput {
    private static Random random;
    private static TupleFactory tupleFactory;
    private static PigStreaming pigStreaming;
    private static int numRecordsPerTestSet;

    public static void main(String[] args) {
        random = new Random();
        tupleFactory = TupleFactory.getInstance();
        pigStreaming = new PigStreaming();
        numRecordsPerTestSet = 100000;
        
        genNumberData();
        genStringData();
        genShallowMapData();
        genDeepMapData();
    }

    private static void genNumberData() {
        try {
            FileOutputStream fos = getFreshFileOutputStream("data/number_data.txt");
            for (int i = 0; i < numRecordsPerTestSet; i++) {
                Tuple t = tupleFactory.newTuple(4);
                t.set(0, new Integer(random.nextInt()));
                t.set(1, new Long(random.nextLong()));
                t.set(2, new Float(random.nextFloat()));
                t.set(3, new Double(random.nextDouble()));
                fos.write(pigStreaming.serialize(t, true, true));
            }
            writeEndOfStreamFlag(fos);
        } catch (FileNotFoundException fnfe) {
            System.out.println(fnfe.toString());
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
        }
    }

    private static void genStringData() {
        try {
            FileOutputStream fos = getFreshFileOutputStream("data/string_data.txt");
            for (int i = 0; i < numRecordsPerTestSet; i++) {
                Tuple t = tupleFactory.newTuple(4);
                for (int j = 0; j < 4; j++) {
                    t.set(j, UUID.randomUUID().toString());
                }
                fos.write(pigStreaming.serialize(t, true, true));
            }
            writeEndOfStreamFlag(fos);
        } catch (FileNotFoundException fnfe) {
            System.out.println(fnfe.toString());
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
        }
    }

    private static void genShallowMapData() {
        try {
            FileOutputStream fos = getFreshFileOutputStream("data/shallow_map_data.txt");
            for (int i = 0; i < numRecordsPerTestSet; i++) {
                Map<String, Object> shallowMap = new HashMap<String, Object>();
                Map<String, Object> nestedMapA = new HashMap<String, Object>();
                Map<String, Object> nestedMapB = new HashMap<String, Object>();

                String uuid = UUID.randomUUID().toString();
                nestedMapA.put(uuid.substring(0, 8), random.nextLong());
                nestedMapA.put(uuid.substring(uuid.length()-8, uuid.length()), random.nextLong());

                uuid = UUID.randomUUID().toString();
                nestedMapB.put(uuid.substring(0, 8), random.nextLong());
                nestedMapB.put(uuid.substring(uuid.length()-8, uuid.length()), random.nextLong());

                shallowMap.put("A", nestedMapA);
                shallowMap.put("B", nestedMapB);

                Tuple t = tupleFactory.newTuple(shallowMap);
                fos.write(pigStreaming.serialize(t, true, true));
            }
            writeEndOfStreamFlag(fos);
        } catch (FileNotFoundException fnfe) {
            System.out.println(fnfe.toString());
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
        }
    }

    private static void genDeepMapData() {
        try {
            FileOutputStream fos = getFreshFileOutputStream("data/deep_map_data.txt");
            for (int i = 0; i < numRecordsPerTestSet; i++) {
                Map<String, Object> deepMap = new HashMap<String, Object>();
                Map<String, Object> nestedMap1 = new HashMap<String, Object>();
                Map<String, Object> nestedMap2 = new HashMap<String, Object>();
                Map<String, Object> nestedMap3 = new HashMap<String, Object>();
                Map<String, Object> nestedMap4 = new HashMap<String, Object>();
                Map<String, Object> nestedMap5 = new HashMap<String, Object>();

                nestedMap5.put("F", random.nextLong());
                nestedMap4.put("E", nestedMap5);
                nestedMap3.put("D", nestedMap4);
                nestedMap2.put("C", nestedMap3);
                nestedMap1.put("B", nestedMap2);
                deepMap.put("A", nestedMap1);

                Tuple t = tupleFactory.newTuple(deepMap);
                fos.write(pigStreaming.serialize(t, true, true));
            }
            writeEndOfStreamFlag(fos);
        } catch (FileNotFoundException fnfe) {
            System.out.println(fnfe.toString());
        } catch (IOException ioe) {
            System.out.println(ioe.toString());
        }
    }

    private static FileOutputStream getFreshFileOutputStream(String path)
                                    throws FileNotFoundException, IOException {
        File f = new File(path);
        f.delete();
        f.createNewFile();
        return new FileOutputStream(f);
    }

    private static void writeEndOfStreamFlag(FileOutputStream fos)
                        throws IOException {
        Tuple t = tupleFactory.newTuple("\u0004");
        fos.write(pigStreaming.serialize(t, true, true));
    }
}