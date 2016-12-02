import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;

/**
 * Created by MK33 on 2016/11/23.
 */
public class AMapTest {

    public static void main(String[] args) throws IOException {

        if (args.length < 0) {
            System.out.println("Please input address");
            System.exit(-1);
        }

        String req = "http://restapi.amap.com/v3/geocode/geo?key=02ea79be41a433373fc8708a00a2c0fa&address=" + args[0];

        URL url = new URL(req);
        URLConnection in = url.openConnection();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in.getInputStream()));
        StringBuffer sb = new StringBuffer();
        String tmp = reader.readLine();
        while (tmp != null) {
            sb.append(tmp);
            tmp = reader.readLine();
        }

        System.out.println(sb.toString());


    }

}
