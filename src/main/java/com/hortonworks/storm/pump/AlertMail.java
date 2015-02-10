package com.hortonworks.storm.pump;

import com.hortonworks.common.util.PropertiesLoader;
import org.apache.log4j.Logger;
import org.codemonkey.simplejavamail.Email;
import org.codemonkey.simplejavamail.Mailer;
import org.codemonkey.simplejavamail.TransportStrategy;

import javax.mail.internet.MimeMessage;
import java.util.Properties;

public class AlertMail {

    private static final Logger LOG = Logger.getLogger(AlertMail.class);

    private static final String GOOGLE_SMTP_SERVER = "smtp.gmail.com";
    private static final int GOOGLE_SMTP_PORT_SSL = 465;

    private static final String PROP_SMTP_USERNAME = "email.smtp.username";
    private static final String PROP_SMTP_PASSWORD = "email.smtp.password";
    private static final String PROP_TO_EMAIL_ADDRESS = "email.to.address";
    private static final String PROP_TO_DISPLAY_NAME = "email.to.displayname";
    private static final String PROP_FROM_EMAIL_ADDRESS = "email.from.address";
    private static final String PROP_FROM_DISPLAY_NAME = "email.from.displayname";
    private static final String PROP_EMAIL_SUBJECT = "email.subject";


    private Properties emailProps = null;

    public AlertMail() {
        String emailConfigFile = "email.properties";
        this.emailProps = null;
        try {
            emailProps = PropertiesLoader.loadPropertiesFromFile(emailConfigFile);
        } catch (Exception ex) {
            LOG.error("Failed to load email config property file.");
            System.exit(-1);
        }
        LOG.info("Email To: " + emailProps.getProperty(PROP_TO_EMAIL_ADDRESS));
        LOG.info("Email From: " + emailProps.getProperty(PROP_FROM_EMAIL_ADDRESS));
    }

    public void sendSmtpMail(String bodyMessage) {

        final Email email = new Email();

        email.setFromAddress(emailProps.getProperty(PROP_FROM_DISPLAY_NAME), emailProps.getProperty(PROP_FROM_EMAIL_ADDRESS));
        email.setSubject(emailProps.getProperty(PROP_EMAIL_SUBJECT));
        email.addRecipient(emailProps.getProperty(PROP_TO_DISPLAY_NAME), emailProps.getProperty(PROP_TO_EMAIL_ADDRESS),
                MimeMessage.RecipientType.TO);
        email.setText(bodyMessage);

        // embed images and include downloadable attachments
//        email.addEmbeddedImage("wink1", imageByteArray, "image/png");
//        email.addEmbeddedImage("wink2", imageDatesource);
//        email.addAttachment("invitation", pdfByteArray, "application/pdf");
//        email.addAttachment("dresscode", odfDatasource);

        Mailer mailer = new Mailer(GOOGLE_SMTP_SERVER, GOOGLE_SMTP_PORT_SSL,
                emailProps.getProperty(PROP_SMTP_USERNAME), emailProps.getProperty(PROP_SMTP_PASSWORD),
                TransportStrategy.SMTP_SSL);
        mailer.sendMail(email);

    }

    public static void main(String[] args) {
        AlertMail driver = new AlertMail();
        String sampleMessage = "Testing SMTP Email driver...";
        driver.sendSmtpMail(sampleMessage);
    }

}
