package com.eventnotification.worker.retry;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Runs the worker as a one-off replay tool instead of a consumer:
 *
 * <pre>
 * java -jar worker.jar --spring.profiles.active=replay --topic=notifications.sms-dlt [--limit=500]
 * </pre>
 *
 * The replay profile turns off the listeners and the web server, so the process exits when done.
 */
@Component
@Profile("replay")
class ReplayCommand implements ApplicationRunner {

    private static final int DEFAULT_LIMIT = 1_000;

    private final DeadLetterReplayer replayer;

    ReplayCommand(DeadLetterReplayer replayer) {
        this.replayer = replayer;
    }

    @Override
    public void run(ApplicationArguments args) {
        String topic = single(args, "topic");
        if (topic == null) {
            throw new IllegalArgumentException("Pass the dead-letter topic with --topic=<name>-dlt");
        }
        String limit = single(args, "limit");
        replayer.replay(topic, limit == null ? DEFAULT_LIMIT : Integer.parseInt(limit));
    }

    private static String single(ApplicationArguments args, String name) {
        List<String> values = args.getOptionValues(name);
        return values == null || values.isEmpty() ? null : values.getFirst();
    }
}
