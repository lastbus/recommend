/**
 * Created by MK33 on 2016/11/17.
 */
public class VolatileTest {

    private static int count = 0;

    public static void inc() {
        try {
            Thread.sleep(10);
        } catch (Exception e) {

        }
        count++;
//        System.out.println(count);
    }

    public static void main(String[] args) {

        for (int i = 0; i < 1000; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    inc();
                }
            }).start();
        }


        System.out.println(count);









    }

}
