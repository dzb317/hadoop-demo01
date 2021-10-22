import org.apache.avro.generic.GenericData;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
        String string = "1    13736230514    192.168.100.14    www.zebin.com    28441    343681    200";
        String[] strings = string.split("    ");
        for (String s :strings
             ) {
            System.out.println(s);

        }
    }
}
