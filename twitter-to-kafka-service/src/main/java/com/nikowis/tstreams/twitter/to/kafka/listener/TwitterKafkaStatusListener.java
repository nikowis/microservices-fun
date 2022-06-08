package com.nikowis.tstreams.twitter.to.kafka.listener;

import com.nikowis.tstreams.config.KafkaConfigData;
import com.nikowis.tstreams.kafka.avro.model.TwitterAvroModel;
import com.nikowis.tstreams.twitter.to.kafka.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.StatusAdapter;

@Component
public class TwitterKafkaStatusListener extends StatusAdapter {

    private static final Logger LOG = LoggerFactory.getLogger(TwitterKafkaStatusListener.class);

    private final KafkaConfigData configData;
    private final KafkaProducer<Long, TwitterAvroModel> kafkaProducer;

    public TwitterKafkaStatusListener(KafkaConfigData configData, KafkaProducer<Long, TwitterAvroModel> kafkaProducer) {
        this.configData = configData;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void onStatus(Status status) {
        LOG.info(status.getText());
        TwitterAvroModel twitterAvroModel = getTwitterAvroModelFromStatus(status);
        kafkaProducer.send(configData.getTopicName(), twitterAvroModel.getUserId(), twitterAvroModel);
    }

    public TwitterAvroModel getTwitterAvroModelFromStatus(Status status) {
        return TwitterAvroModel
                .newBuilder()
                .setId(status.getId())
                .setUserId(status.getUser().getId())
                .setText(status.getText())
                .setCreatedAt(status.getCreatedAt().getTime())
                .build();
    }

}
