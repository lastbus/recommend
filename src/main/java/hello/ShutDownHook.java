package hello;

/**
 * Created by MK33 on 2016/5/29.
 */
public class ShutDownHook {

    public static void main(String[] args)
    {
        Runtime.getRuntime().addShutdownHook(
                new Thread() {
                    public void run() { System.out.println("I'm shutdown hook 1");}
                });
        Runtime.getRuntime().addShutdownHook(
                new Thread() {
                    public void run() { System.out.println("I'm shutdown hook 2");}
                });
        Runtime.getRuntime().addShutdownHook(
                new Thread() {
                    public void run() { System.out.println("I'm shutdown hook 3");}
                });
        System.out.println("Hello world");
        Runtime.getRuntime().addShutdownHook(
                new Thread() {
            public void run() { System.out.println("I'm shutdown hook 4");}
        });
    }
}
