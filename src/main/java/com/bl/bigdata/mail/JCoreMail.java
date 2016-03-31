package com.bl.bigdata.mail;

import javax.mail.*;
import javax.mail.Message;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Properties;

/**
 * Created by Administrator on 2016/1/29.
 */
public class JCoreMail {

    public static void main(String args[]) throws IOException, MessagingException, GeneralSecurityException {
//        MailSSLSocketFactory sf = new MailSSLSocketFactory();
        Properties properties = new Properties();
        try(InputStream in = Files.newInputStream(Paths.get("mail.properties"))){
            properties.load(in);
        }
//        properties.put("mail.smtp.ssl.enable", "true");
//        properties.put("mail.smtp.ssl.socketFactory", sf);
//        properties.put("mail.smtp.starttls.enable","true" );
//        properties.put("mail.smtp.auth", "true");
//        properties.put("mail.smtp.socketFactory.port", "587");
//        properties.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
//        properties.put("mail.smtp.socketFactory.fallback", "false");
//        properties.setProperty("mail.imap.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
//        properties.setProperty("mail.imap.socketFactory.fallback", "false");
//        properties.setProperty("mail.imap.port", "993");
//        properties.setProperty("mail.imap.socketFactory.port", "993");
//        List<String> lines = Files.readAllLines(Paths.get(args[0]), Charset.forName("UTF-8"));

        String from = "819307659@qq.com"; //lines.get(0);
        String to = "Ke.Ma@bl.com";//lines.get(1);
        String subject = "qq to qq"; //lines.get(2);

        System.out.println(0);
        Session mailSession = Session.getInstance(properties);
//        PopupAuthenticator popAuthenticator = new PopupAuthenticator();
//        PasswordAuthentication pop = popAuthenticator.performCheck("xxxxxxxxx", "passwrord");
        System.out.println(1);
        mailSession.setDebug(true);
        MimeMessage message = new MimeMessage(mailSession);
        message.setFrom(new InternetAddress(from));
        message.setRecipient(Message.RecipientType.TO, new InternetAddress(to));
        message.setSubject(subject);
        message.setText("builder.toString()");
        Transport tr = mailSession.getTransport();
        try{
            System.out.println(2);
            tr.connect("smtp.qq.com", from, "kspowryistznbeih");
            System.out.println(3);
            tr.sendMessage(message, message.getAllRecipients());
            System.out.println(4);
        }finally {
            tr.close();
        }









    }
}
