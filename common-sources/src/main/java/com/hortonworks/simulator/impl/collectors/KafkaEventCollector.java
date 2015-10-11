package com.hortonworks.simulator.impl.collectors;

import com.hortonworks.simulator.impl.domain.AbstractEventCollector;
import com.hortonworks.simulator.impl.domain.transport.MobileEyeEvent;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class KafkaEventCollector extends AbstractEventCollector {

  private static final String TOPIC = "truck_events";
  private Producer<String, String> kafkaProducer;
  private Properties props = new Properties();

  public KafkaEventCollector() {
    Properties properties = new Properties();
    try {
      properties.load(new FileInputStream(new File("/etc/storm_demo/config.properties")));
    } catch (Exception ex) {
      System.err.println("Unable to locate config file: /etc/storm_demo/config.properties");
      System.exit(0);
    }

    props.put("metadata.broker.list", properties.getProperty("kafka.brokers"));
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");

    try {
      ProducerConfig producerConfig = new ProducerConfig(props);
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

    logger.info("Creating event[" + eventToPass + "] for driver[" + driverId + "] in truck [" + mee.getTruck() + "]");

    try {
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, driverId, eventToPass);
      kafkaProducer.send(data);
    } catch (Exception e) {
      logger.error("Error sending event[" + eventToPass + "] to Kafka queue (" + props.get("metadata.broker.list") +
          ")", e);
    }
  }

}
