package com.eventnotification.worker.delivery;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.util.Optional;

@Component
public class MessageParser {

    private final JsonMapper jsonMapper;

    public MessageParser(JsonMapper jsonMapper) {
        this.jsonMapper = jsonMapper;
    }

    /**
     * @throws MalformedMessageException for empty values, the JSON literal null, invalid JSON
     *                                   and messages without a notification ID
     */
    public NotificationMessage parse(ConsumerRecord<String, String> record) {
        return parse(record.value(), record.topic() + "-" + record.partition() + "@" + record.offset());
    }

    public Optional<NotificationMessage> tryParse(ConsumerRecord<String, String> record) {
        try {
            return Optional.of(parse(record));
        } catch (MalformedMessageException e) {
            return Optional.empty();
        }
    }

    /** Like {@link #tryParse(ConsumerRecord)}, for a record value on its own. */
    public Optional<NotificationMessage> tryParse(String value) {
        try {
            return Optional.of(parse(value, "an unknown offset"));
        } catch (MalformedMessageException e) {
            return Optional.empty();
        }
    }

    private NotificationMessage parse(String value, String where) {
        if (value == null || value.isBlank()) {
            throw new MalformedMessageException("Empty message at " + where, null);
        }
        NotificationMessage message;
        try {
            message = jsonMapper.readValue(value, NotificationMessage.class);
        } catch (JacksonException e) {
            throw new MalformedMessageException("Invalid JSON at " + where + ": " + e.getOriginalMessage(), e);
        }
        if (message == null || message.notificationId() == null) {
            throw new MalformedMessageException("Message without a notification ID at " + where, null);
        }
        return message;
    }
}
