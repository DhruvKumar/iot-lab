package com.hortonworks.lab;

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

    // Lab: Send the event to kafkaProducer and log the message

    //  1. cast event to MobileEyeEvent
    MobileEyeEvent mee = (MobileEyeEvent) event;

    //  2. get eventToPass from MobileEyeEvent created above
    String eventToPass = mee.toString();

    //  3. similarly get driverID from MobileEyeEvent
    String driverId = ""; // implement me

    logger.debug("Creating event[" + eventToPass + "] for driver[" + driverId + "] in truck [" + mee.getTruck() + "]");

    // 4. create KeyedMessage with topic as "truck_events", key as driverId, value as eventToPass
    KeyedMessage<String, String> msg = null; // implement me

    try {

    // 5. send msg using KafkaProducer (make sure you've implemented step 4 above to avoid NPE)
      kafkaProducer.send(msg);
    } catch (Exception e) {
      logger.error("Error sending event[" + eventToPass + "] to Kafka queue (" +
          kafkaProperties.get("metadata.broker.list") + ")", e);
    }
  }
}
