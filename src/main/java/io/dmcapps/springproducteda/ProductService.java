package io.dmcapps.springproducteda;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import io.dmcapps.proto.Brand;
import io.dmcapps.proto.Category;
import io.dmcapps.proto.Product;
import io.dmcapps.proto.Product.Builder;
import io.dmcapps.proto.Product.Status;

@Component
class ProductService {

    private static final Logger log = LoggerFactory.getLogger(ProductService.class);

    private static final String INPUT_PRODUCTS_TOPIC = "in-products";
    private static final String PRODUCTS_TOPIC = "products";
    private static final String PRODUCTS_STORE = "products-store";
    private static final String BRANDS_TOPIC = "brands";
    private static final String BRANDS_STORE = "brands-store";
    private static final String CATEGORIES_TOPIC = "categories";
    private static final String CATEGORIES_STORE = "categories-store";
    
    @Value("${spring.kafka.properties.schema.registry.url}")
    String srUrl;

    @Bean
    public NewTopic inputProductsTopic() {
        return new NewTopic(INPUT_PRODUCTS_TOPIC, 3, (short) 1);
    }

    @Bean
    public NewTopic productsTopic() {
        return new NewTopic(PRODUCTS_TOPIC, 3, (short) 1);
    }

    @Autowired
    public void process(StreamsBuilder builder, StreamsBuilderFactoryBean streamsBuilderFB) {
        
        builder.
            stream(BRANDS_TOPIC, Consumed.with(specificBrandProto(), specificBrandProto()))
            .toTable(Materialized.as(BRANDS_STORE));
        builder
            .stream(CATEGORIES_TOPIC, Consumed.with(specificCategoryProto(), specificCategoryProto()))
            .toTable(Materialized.as(CATEGORIES_STORE));

        
        StoreQueryParameters<ReadOnlyKeyValueStore<Product, Product>> productsStoreQueryParam = StoreQueryParameters
        .fromNameAndType(PRODUCTS_STORE, QueryableStoreTypes.keyValueStore());

        
        StoreQueryParameters<ReadOnlyKeyValueStore<Brand, Brand>> brandsStoreQueryParam = StoreQueryParameters
        .fromNameAndType(BRANDS_STORE, QueryableStoreTypes.keyValueStore());

        
        StoreQueryParameters<ReadOnlyKeyValueStore<Category, Category>> categoriesStoreQueryParam = StoreQueryParameters
        .fromNameAndType(CATEGORIES_STORE, QueryableStoreTypes.keyValueStore());
        
        
        KStream<Product, Product> inputProductsStream = builder
            .stream(INPUT_PRODUCTS_TOPIC, Consumed.with(specificProto(), specificProto()));

        KStream<Product, Product> validatedProductsStream = inputProductsStream
            .map((key, product) -> {
                log.info(product.toString());
                Builder productBuilder = product.toBuilder();

                ReadOnlyKeyValueStore<Product, Product> productsStore = streamsBuilderFB
                    .getKafkaStreams()
                    .store(productsStoreQueryParam);
                
                Product productKey = Product.newBuilder().setName(product.getName()).setBrand(product.getBrand()).build();
                Product storedProduct = productsStore.get(productKey);
                
                ReadOnlyKeyValueStore<Brand, Brand> brandsStore = streamsBuilderFB
                    .getKafkaStreams()
                    .store(brandsStoreQueryParam);
                Brand brandKey = Brand.newBuilder().setName(product.getBrand().getName()).build();
                Brand storedBrand = brandsStore.get(brandKey);

                ReadOnlyKeyValueStore<Category, Category> categoriesStore = streamsBuilderFB
                    .getKafkaStreams()
                    .store(categoriesStoreQueryParam);
                Category categoryKey = Category.newBuilder().setName(product.getCategory().getName()).setParent(product.getCategory().getParent()).build();
                Category storedCategory = categoriesStore.get(categoryKey);


                if (storedBrand == null || storedCategory == null || storedProduct == null){
                    productBuilder.setStatus(Status.REJECTED);
                    return new KeyValue<>(productKey, productBuilder.build());
                }
                
                if (storedProduct.getStatus() == Status.PENDING || storedProduct.getStatus() == Status.REJECTED) {
                    String seed = product.getName()+product.getBrand().getName();
                    final String uuid = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
                    productBuilder.setId(uuid).setStatus(Status.CREATED);
                    return new KeyValue<>(productKey, productBuilder.build());
                }
                if (storedProduct.getStatus() == Status.CREATED || storedProduct.getStatus() == Status.UPDATED) {
                    productBuilder.setStatus(Status.UPDATED);
                    return new KeyValue<>(productKey, productBuilder.build());
                }
                return new KeyValue<>(productKey, productBuilder.build());

            });
        validatedProductsStream.to(PRODUCTS_TOPIC, Produced.with(specificProto(), specificProto()));
        validatedProductsStream.toTable(Materialized.as(PRODUCTS_STORE));
    }

    private KafkaProtobufSerde<Product> specificProto() {

        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);

        final KafkaProtobufSerde<Product> kafkaProtobufSerde = new KafkaProtobufSerde<>(Product.class);
        kafkaProtobufSerde.configure(config, false);
        return kafkaProtobufSerde;
    }

    private KafkaProtobufSerde<Brand> specificBrandProto() {

        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);

        final KafkaProtobufSerde<Brand> kafkaProtobufSerde = new KafkaProtobufSerde<>(Brand.class);
        kafkaProtobufSerde.configure(config, false);
        return kafkaProtobufSerde;
    }

    private KafkaProtobufSerde<Category> specificCategoryProto() {

        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);

        final KafkaProtobufSerde<Category> kafkaProtobufSerde = new KafkaProtobufSerde<>(Category.class);
        kafkaProtobufSerde.configure(config, false);
        return kafkaProtobufSerde;
    }

}

@Component
class TestProductProducer {

  private final KafkaTemplate<Product, Product> kafkaTemplate;

  @Autowired
  TestProductProducer(KafkaTemplate<Product, Product> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }


  @EventListener(ApplicationStartedEvent.class)
  public void produceMovies() {

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
  public void consume(ConsumerRecord<Product, Product> record) {
    log.info(record.value().toString());
  }
}