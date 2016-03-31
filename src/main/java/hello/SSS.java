package hello;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Created by MK33 on 2016/3/29.
 */
@Component
public class SSS {

    @Scheduled(fixedRate = 5000)
    public void println(){
        System.out.println("Hello world!");
    }
}
