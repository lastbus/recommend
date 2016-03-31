package com.bl.bigdata.mail;

import javax.mail.*;
import javax.mail.event.FolderEvent;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by MK33 on 2016/1/29.
 */
public class SendMail {

    public static void main(String[] args) throws MessagingException, IOException {
        //配置发送邮件的环境属性
        final Properties properties = new Properties();
        try (InputStream reader = Files.newInputStream(Paths.get("mail.properties"))){
            properties.load(reader);
        }

        Authenticator authenticator = new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                String userName = properties.getProperty("mail.user");
                String password = properties.getProperty("mail.password");
                return new PasswordAuthentication(userName, password);
            }
        };

        Session mailSession = Session.getInstance(properties, authenticator);
        MimeMessage message = new MimeMessage(mailSession);
        mailSession.setDebug(true);
        //设置发件人
        InternetAddress from = new InternetAddress(properties.getProperty("mail.user"));
        message.setFrom(from);

        //设置收件人
        InternetAddress to = new InternetAddress("Ke.Ma@bl.com");
        message.setRecipient(MimeMessage.RecipientType.TO, to);
        message.setText("测试邮件");
        message.setSubject("test from sina");
        message.setContent("<a href='http://www.baidu.com'>测试的HTML邮件</a>", "text/html; charset=UTF-8");
        Transport.send(message);
    }
}
