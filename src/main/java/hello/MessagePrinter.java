package hello;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by MK33 on 2016/3/18.
 */
@Component
public class MessagePrinter {

    final private MessageService service;

    @Autowired
    public MessagePrinter(MessageService service){
        this.service = service;
    }

    public void printMessage(){
        System.out.println(this.service.getMessage());
    }
}
