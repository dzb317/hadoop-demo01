import org.apache.avro.generic.GenericData;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        String a = "我特发";
        byte[] bytes = a.getBytes();
        for (byte b:bytes
             ) {
            System.out.println(b);
        }
    }
}
