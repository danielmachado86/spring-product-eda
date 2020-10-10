package io.dmcapps.springproducteda;

import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import io.dmcapps.proto.Brand;
import io.dmcapps.proto.Category;
import io.dmcapps.proto.Product;
import io.dmcapps.proto.Product.Status;

@Component
class ProductStreamManager {

    @Value("${spring.kafka.properties.schema.registry.url}")
    String srUrl;

    private static final String PRODUCTS_TOPIC = "products";
    private static final String PRODUCTS_STORE = "products-store";

    private final KafkaTemplate<String, Product> kafkaTemplate;

    @Autowired
    StreamsBuilder builder;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFB;

    @Autowired
    ProductStreamManager(KafkaTemplate<String, Product> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void produce(Product product) {
        kafkaTemplate.send("in-products", product);
    }

    public ReadOnlyKeyValueStore<String, Product> getProductStore() {
        
        StoreQueryParameters<ReadOnlyKeyValueStore<String, Product>> productsStoreQueryParam = StoreQueryParameters
        .fromNameAndType(PRODUCTS_STORE, QueryableStoreTypes.keyValueStore());
        
        while (true) {
            try {
                return streamsBuilderFB.getKafkaStreams().store(productsStoreQueryParam);
            } catch (InvalidStateStoreException e) {
                // ignore, store not ready yet
            }
        }
    }

}

@Component
class TestProductProducer {

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

    Stream.of(message1, message2, message3, message4).forEach(product -> kafkaTemplate.send("in-products", product));

  }
}

@Component
class TestProductConsumer {

  private static final Logger log = LoggerFactory.getLogger(TestProductConsumer.class);

  @KafkaListener(topics = { "products" }, groupId = "products_listener")
  public void consume(ConsumerRecord<String, Product> record) {
    log.info(record.value().toString());
  }
}