package io.dmcapps.springproducteda;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import io.dmcapps.proto.catalog.Brand;



@Component
class BrandStreamManager {

  private static final Logger log = LoggerFactory.getLogger(BrandStreamManager.class);

  @KafkaListener(topics = { "in-brands" }, groupId = "brands_listener")
  public void consume(ConsumerRecord<String, Brand> record) {
    log.info("Brand: {}", record.value());
  }
}