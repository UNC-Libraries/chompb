package edu.unc.lib.boxc.migration.cdm.services;

import org.slf4j.Logger;

import jakarta.mail.Message;
import jakarta.mail.MessagingException;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Service for sending email notifications
 * @author bbpennel
 */
public class EmailService {
    private static final Logger log = getLogger(EmailService.class);

    private String smtpHost;
    private int smtpPort = 25;

    /**
     * Send a plain-text email to one or more recipients.
     * @param subject email subject
     * @param body email body
     * @param fromAddress sender address
     * @param toAddresses one or more recipient addresses
     */
    public void sendEmail(String subject, String body, String fromAddress, String... toAddresses) {
        Properties props = new Properties();
        props.put("mail.smtp.host", smtpHost);
        props.put("mail.smtp.port", String.valueOf(smtpPort));

        Session session = Session.getInstance(props);
        try {
            MimeMessage message = new MimeMessage(session);
            message.setFrom(new InternetAddress(fromAddress));
            for (String to : toAddresses) {
                message.addRecipient(Message.RecipientType.TO, new InternetAddress(to));
            }
            message.setSubject(subject);
            message.setText(body);
            Transport.send(message);
            log.info("Email sent: subject='{}' to={}", subject, toAddresses);
        } catch (MessagingException e) {
            log.error("Failed to send email with subject '{}': {}", subject, e.getMessage(), e);
        }
    }

    /**
     * Format a Throwable's message and full stack trace as a string
     * @param t the throwable
     * @return formatted string
     */
    public static String formatException(Throwable t) {
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    public void setSmtpHost(String smtpHost) {
        this.smtpHost = smtpHost;
    }

    public void setSmtpPort(int smtpPort) {
        this.smtpPort = smtpPort;
    }
}

