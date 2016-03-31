package hello;

import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Created by MK33 on 2016/3/18.
 */
@Configuration
@ComponentScan
@EnableScheduling
public class Application {

    @Bean
    MessageService mockMessageService() {
        return new MessageService() {
            public String getMessage() {
                return "Hello World!";
            }
        };
    }
    public static void main(String args[]){

        SpringApplication.run(Application.class);
//        ApplicationContext context = new AnnotationConfigApplicationContext(Application.class);
//        MessagePrinter printer = context.getBean(MessagePrinter.class);
//        printer.printMessage();
    }
}
