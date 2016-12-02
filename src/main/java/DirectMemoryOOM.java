import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by MK33 on 2016/11/7.
 */
public class DirectMemoryOOM {


    public static void main(String[] args) throws IllegalAccessException {
        System.out.println("Hello world");
        if (args.length > 0)System.out.println(args[0]);
    }
}
