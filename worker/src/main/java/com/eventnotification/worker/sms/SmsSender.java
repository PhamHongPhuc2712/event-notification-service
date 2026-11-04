package com.eventnotification.worker.sms;

import com.eventnotification.worker.config.WorkerProperties;
import com.eventnotification.worker.delivery.ChannelSender;
import com.eventnotification.worker.delivery.FailureCodes;
import com.eventnotification.worker.delivery.NotificationMessage;
import com.eventnotification.worker.delivery.PermanentDeliveryException;
import com.eventnotification.worker.delivery.TransientDeliveryException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.net.http.HttpClient;

/**
 * Sends SMS through the provider's HTTP API.
 *
 * <p>The notification ID is sent as the provider's {@code Idempotency-Key}. A timeout does not
 * mean the SMS was not sent: the provider may have accepted it and the response got lost. The
 * retry sends the same key, so the provider does not text the user twice.
 */
@Component
public class SmsSender implements ChannelSender {

    static final String IDEMPOTENCY_KEY = "Idempotency-Key";

    record SendSmsRequest(String to, String body) {
    }

    private final RestClient client;

    public SmsSender(WorkerProperties properties) {
        WorkerProperties.Sms sms = properties.sms();
        HttpClient httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(sms.connectTimeout())
                .build();
        JdkClientHttpRequestFactory requestFactory = new JdkClientHttpRequestFactory(httpClient);
        requestFactory.setReadTimeout(sms.readTimeout());
        this.client = RestClient.builder()
                .baseUrl(sms.baseUrl().toString())
                .requestFactory(requestFactory)
                .build();
    }

    @Override
    public void send(NotificationMessage message) {
        try {
            client.post()
                    .uri("/v1/messages")
                    .header(IDEMPOTENCY_KEY, message.notificationId().toString())
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(new SendSmsRequest(message.recipient(), message.body()))
                    .retrieve()
                    .toBodilessEntity();
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().isSameCodeAs(HttpStatus.TOO_MANY_REQUESTS)
                    || e.getStatusCode().isSameCodeAs(HttpStatus.REQUEST_TIMEOUT)) {
                throw transientError(message, e);
            }
            String code = e.getStatusCode().isSameCodeAs(HttpStatus.BAD_REQUEST)
                    ? FailureCodes.RECIPIENT_REJECTED
                    : FailureCodes.REJECTED_BY_PROVIDER;
            throw new PermanentDeliveryException(code, "SMS provider rejected notification "
                    + message.notificationId() + " with " + e.getStatusCode(), e);
        } catch (RestClientException e) {
            // 5xx responses, connection errors and timeouts.
            throw transientError(message, e);
        }
    }

    private static TransientDeliveryException transientError(NotificationMessage message, Exception e) {
        return new TransientDeliveryException("SMS provider unavailable for notification "
                + message.notificationId(), e);
    }
}
