package iot;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Date;
import java.util.Properties;

public class NotificationManager {
    private String to = System.getenv("GMAIL_TO");
    private String from = System.getenv("GMAIL_FROM");
    private Session session;


    public NotificationManager() {
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", "smtp.gmail.com");
        props.put("mail.smtp.port", "587");

        this.session = Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(from, System.getenv("GMAIL_PASSWORD"));
            }
        });
    }

    public void sendNotification(float threshold, float currentValue) throws MessagingException {
        String msgText = "The temperature in your room is above the defined threshold.\n"
                + "The current value of the threshold is " + threshold + "ºC and the current temperature is " + currentValue + "ºC.";

        // create a message
        MimeMessage msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress(from));
        InternetAddress[] address = {new InternetAddress(to)};
        msg.setRecipients(Message.RecipientType.TO, address);
        msg.setSubject("[ALERT] Temperature above threshold!");
        msg.setSentDate(new Date());
        // If the desired charset is known, you can use
        // setText(text, charset)
        msg.setText(msgText);

        Transport.send(msg);
    }
}