package io.dmcapps.springproducteda;

import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import io.dmcapps.proto.Brand;
import io.dmcapps.proto.Brand.Status;



@Component
class BrandStreamManager {

    private static final Logger log = LoggerFactory.getLogger(BrandStreamManager.class);

    private final KafkaTemplate<String, Brand> kafkaTemplate;

    @Autowired
    BrandStreamManager(KafkaTemplate<String, Brand> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    public void produce(Brand brand) {
        kafkaTemplate.send("in-brands", brand);
    }

}

@Component
class TestBrandProducer {

  private final KafkaTemplate<String, Brand> kafkaTemplate;

  @Autowired
  TestBrandProducer(KafkaTemplate<String, Brand> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @EventListener(ApplicationStartedEvent.class)
  public void produceMovies() {
    final Brand message1= Brand.newBuilder().setName("Ramo").setStatus(Status.PENDING).build();
    final Brand message2 = Brand.newBuilder().setName("Noel").setStatus(Status.PENDING).build();

    Stream.of(message1, message2).forEach(brand -> kafkaTemplate.send("in-brands", brand));

  }
}

@Component
class TestBrandConsumer {

  private static final Logger log = LoggerFactory.getLogger(Consumer.class);

  @KafkaListener(topics = { "brands" }, groupId = "brands_listener")
  public void consume(ConsumerRecord<String, Brand> record) {
    log.info(record.value().toString());
  }
}