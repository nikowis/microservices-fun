package com.nikowis.microservices.twitter.to.kafka.runner.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nikowis.microservices.twitter.to.kafka.config.TwitterToKafkaServiceConfigData;
import com.nikowis.microservices.twitter.to.kafka.exception.BusinessException;
import com.nikowis.microservices.twitter.to.kafka.listener.TwitterKafkaStatusListener;
import com.nikowis.microservices.twitter.to.kafka.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockStreamRunnerImpl implements StreamRunner {

    public static final Logger LOG = LoggerFactory.getLogger(MockStreamRunnerImpl.class);

    private final TwitterToKafkaServiceConfigData config;
    private final TwitterKafkaStatusListener listener;
    private final ObjectMapper mapper;

    private static final Integer TWEET_MIN_LEN = 5;
    private static final Integer TWEET_MAX_LEN = 20;
    private static final Random RANDOM = new Random();

    private static final String DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";
    private static final String[] WORDS = new String[]{
            "A", "B", "C", "D", "E", "F",
            "G", "H", "I", "J", "K", "L",
            "M", "N", "O", "P", "Q", "R",
            "S", "T", "U", "V", "W", "X",
            "Y", "Z"
    };

    public static class MockTweet {
        String createdAt;
        String id;
        String text;
        MockUser user;

        public static class MockUser {
            String id;
        }
    }

    public MockStreamRunnerImpl(TwitterToKafkaServiceConfigData config,
                                TwitterKafkaStatusListener listener,
                                ObjectMapper mapper) {
        this.config = config;
        this.listener = listener;
        this.mapper = mapper;
    }

    @Override
    public void start() {
        Long mockSleepMs = config.getMockSleepMs();
        LOG.info("Starting mock twitter stream");
        simulateStreaming(mockSleepMs);
    }

    private void simulateStreaming(Long mockSleepMs) {
        Executors.newSingleThreadExecutor().submit(() -> {
            while (true) {
                Status status = TwitterObjectFactory.createStatus(createNewTweetJson());
                listener.onStatus(status);
                sleep(mockSleepMs);
            }
        });
    }

    private void sleep(Long mockSleepMs) {
        try {
            Thread.sleep(mockSleepMs);
        } catch (InterruptedException e) {
            throw new BusinessException("Error while sleeping", e);
        }
    }

    private String createNewTweetJson() {
        MockTweet tweet = new MockTweet();
        tweet.createdAt = ZonedDateTime.now().format(DateTimeFormatter.ofPattern(DATE_FORMAT, Locale.ENGLISH));
        tweet.id = String.valueOf(ThreadLocalRandom.current().nextLong());
        tweet.text = createRandomTweetText();
        tweet.user = new MockTweet.MockUser();
        tweet.user.id = String.valueOf(ThreadLocalRandom.current().nextLong());

        String json = null;
        try {
            json = mapper.writeValueAsString(tweet);
        } catch (JsonProcessingException e) {
            LOG.error("Error parsing mock tweet to json", e);
            throw new BusinessException("Can't parse json", e);
        }
        return json;
    }

    private String createRandomTweetText() {
        StringBuilder sb = new StringBuilder();
        int tweetLen = RANDOM.nextInt(TWEET_MIN_LEN, TWEET_MAX_LEN);
        for (int i = 0; i < tweetLen; i++) {
            sb.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
        }

        return sb.toString().trim();
    }

}
