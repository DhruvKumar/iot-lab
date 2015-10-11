package com.hortonworks.solution;

import akka.actor.UntypedActor;
import com.hortonworks.simulator.impl.domain.transport.MobileEyeEvent;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Properties;

public class KafkaSensorEventCollector extends UntypedActor {

  private static final String TOPIC = "truck_events";
  private Producer<String, String> kafkaProducer;
  private Properties props = new Properties();
  private Properties kafkaProperties = Lab.kafkaLocalBroker.getKafkaProperties();
  private Logger logger = Logger.getLogger(this.getClass());

  public KafkaSensorEventCollector() {
    kafkaProperties.put("metadata.broker.list", "localhost:20111");
    kafkaProperties.put("serializer.class", "kafka.serializer.StringEncoder");
    //kafkaProperties.put("request.required.acks", "1");
    try {
      ProducerConfig producerConfig = new ProducerConfig(kafkaProperties);
      kafkaProducer = new Producer<String, String>(producerConfig);
    } catch (Exception e) {
      logger.error("Error creating producer", e);
    }
  }

  @Override
  public void onReceive(Object event) throws Exception {
    MobileEyeEvent mee = (MobileEyeEvent) event;
    String eventToPass = mee.toString();
    String driverId = String.valueOf(mee.getTruck().getDriver().getDriverId());

    logger.debug("Creating event[" + eventToPass + "] for driver[" + driverId + "] in truck [" + mee.getTruck() + "]");

    try {
      KeyedMessage<String, String> msg = new KeyedMessage<String, String>(TOPIC, driverId, eventToPass);
      kafkaProducer.send(msg);
    } catch (Exception e) {
      logger.error("Error sending event[" + eventToPass + "] to Kafka queue (" +
          kafkaProperties.get("metadata.broker.list") + ")", e);
    }
  }
}
