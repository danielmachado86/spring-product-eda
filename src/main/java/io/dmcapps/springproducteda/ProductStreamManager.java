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

import io.dmcapps.proto.catalog.Brand;
import io.dmcapps.proto.catalog.Category;
import io.dmcapps.proto.catalog.Product;
import io.dmcapps.proto.catalog.Product.Status;

@Component
class ProductStreamManager {

    private static final String INPUT_PRODUCTS_TOPIC = "in-products";
    
    
    private final KafkaTemplate<String, Product> kafkaTemplate;
    
    @Autowired
    ProductStreamManager(KafkaTemplate<String, Product> kafkaTemplate) {
      this.kafkaTemplate = kafkaTemplate;
    }
    
    public void produce(String key) {
      kafkaTemplate.send(INPUT_PRODUCTS_TOPIC, key, null);
    }
    
    public void produce(String key, Product product) {
      kafkaTemplate.send(INPUT_PRODUCTS_TOPIC, key, product);
    }
    
  }
  
  @Component
  class TestProductProducer {
    
    private static final String INPUT_PRODUCTS_TOPIC = "in-products";
    
    private final KafkaTemplate<String, Product> kafkaTemplate;

  @Autowired
  TestProductProducer(KafkaTemplate<String, Product> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }


  @EventListener(ApplicationStartedEvent.class)
  public void produce() {

    final Brand brand1 = Brand.newBuilder().setName("Ramo").build();
    final Brand brand2 = Brand.newBuilder().setName("Noel").build();
    
    final Category category1 = Category.newBuilder().setName("Postres").setParent("").build();
    final Category category2 = Category.newBuilder().setName("Dulces").setParent("").build();

    final Product message1= Product.newBuilder().setName("Chocoramo").setBrand(brand1).setCategory(category1).setStatus(Status.PENDING).build();
    final Product message2 = Product.newBuilder().setName("Barra de Chocoramo").setBrand(brand1).setCategory(category1).setStatus(Status.PENDING).build();
    final Product message3 = Product.newBuilder().setName("Gansito").setBrand(brand1).setCategory(category1).setStatus(Status.PENDING).build();
    final Product message4 = Product.newBuilder().setName("Caramelos").setBrand(brand2).setCategory(category2).setStatus(Status.PENDING).build();

    Stream.of(message1, message2, message3, message4).forEach(product -> kafkaTemplate.send(INPUT_PRODUCTS_TOPIC, product));

  }
}

@Component
class TestProductConsumer {

  private static final Logger log = LoggerFactory.getLogger(TestProductConsumer.class);

  @KafkaListener(topics = { "products" }, groupId = "products_listener")
  public void consume(ConsumerRecord<String, Product> record) {
    log.info("Product: {}", record.value());
  }
}