package com.hortonworks.lab;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.hortonworks.stormprocessors.TruckEventRuleEngine;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

public class TruckEventRuleBolt implements IRichBolt {


  private static final long serialVersionUID = 6816706717943954742L;
  private static final Logger LOG = Logger.getLogger(TruckEventRuleBolt.class);

  private OutputCollector collector;

  public static final int MAX_UNSAFE_EVENTS = 5;
  public Map<Integer, LinkedList<String>> driverEvents = new HashMap<Integer, LinkedList<String>>();
  private long lastCorrelationId;


  @Override
  public void prepare(Map stormConf, TopologyContext context,
                      OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    int driverId = input.getIntegerByField("driverId");
    String driverName = input.getStringByField("driverName");
    int routeId = input.getIntegerByField("routeId");
    String routeName = input.getStringByField("routeName");
    int truckId = input.getIntegerByField("truckId");
    Timestamp eventTime = (Timestamp) input.getValueByField("eventTime");
    String eventType = input.getStringByField("eventType");
    double longitude = input.getDoubleByField("longitude");
    double latitude = input.getDoubleByField("latitude");
    long correlationId = input.getLongByField("correlationId");


    LOG.info("Processing truck event[" + eventType + "]  for driverId[" + driverId + "], truck[" + truckId + "], " +
        "route[" + routeName + "], correlationId[" + correlationId + "]");
    processEvent(driverId, driverName, routeId, truckId, eventTime, eventType, longitude, latitude,
        correlationId, routeName);
    collector.ack(input);
  }


  public void processEvent(int driverId, String driverName, int routeId, int truckId, Timestamp eventTime, String
      event, double longitude, double latitude, long currentCorrelationId, String routeName) {

    if (lastCorrelationId != currentCorrelationId) {
      lastCorrelationId = currentCorrelationId;
      driverEvents.clear();
    }

    if (!driverEvents.containsKey(driverId)) {
      driverEvents.put(driverId, new LinkedList<String>());
    }

    if (!event.equals("Normal")) {
      if (driverEvents.get(driverId).size() < MAX_UNSAFE_EVENTS) {
        driverEvents.get(driverId).push(eventTime + " " + event);
        LOG.info("Driver[" + driverId + "] " + driverName + " had an unsafe event. Total unsafe " +
            "events: " + driverEvents.get(driverId).size());
      } else {
        LOG.info("Driver[" + driverId + "] has exceed max events...");
        LOG.info("UNSAFE DRIVING DETECTED FOR DRIVER ID: " + driverId);
        StringBuffer events = new StringBuffer();
        for (String unsafeEvent : driverEvents.get(driverId)) {
          events.append(unsafeEvent + "\n");
        }
        LOG.info("Clearing violations table for Driver[" + driverId + "]");
        driverEvents.get(driverId).clear();
      }
    }
  }


  @Override
  public void cleanup() {

  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}