package io.dmcapps.springproducteda;

import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
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
import io.dmcapps.proto.Category;
import io.dmcapps.proto.Category.Status;


@Component
class CategoryService {

    private static final Logger log = LoggerFactory.getLogger(CategoryService.class);

    private static final String INPUT_CATEGORIES_TOPIC = "in-categories";
    private static final String CATEGORIES_TOPIC = "categories";
    private static final String CATEGORIES_STORE = "categories-store";


    @Value("${spring.kafka.properties.schema.registry.url}")
    String srUrl;

    @Bean
    public NewTopic inputCategoriesTopic() {
        return new NewTopic(INPUT_CATEGORIES_TOPIC, 3, (short) 1);
    }

    @Bean
    public NewTopic categoriesTopic() {
        return new NewTopic(CATEGORIES_TOPIC, 3, (short) 1);
    }

    @Autowired
    public void process(StreamsBuilder builder, StreamsBuilderFactoryBean streamsBuilderFB) {
        builder
            .stream(INPUT_CATEGORIES_TOPIC, Consumed.with(specificProto(), specificProto()))
            .map((key, category) -> {
                StoreQueryParameters<ReadOnlyKeyValueStore<Category, Category>> sqp = StoreQueryParameters
                    .fromNameAndType(CATEGORIES_STORE, QueryableStoreTypes.keyValueStore());

                Category categoryKey = Category.newBuilder().setName(category.getName()).setParent(category.getParent()).build();

                Category storedCategory = streamsBuilderFB
                    .getKafkaStreams()
                    .store(sqp)
                    .get(categoryKey);
                log.info(category.toString());
                Category processedCategory = null;
                if (storedCategory != null) {
                    if (storedCategory.getStatus() != Status.CREATED
                            || storedCategory.getStatus() != Status.UPDATED) {
                        processedCategory = category
                            .toBuilder()
                            .setStatus(Status.UPDATED)
                            .build();
                    }
                } else {
                    processedCategory = category
                        .toBuilder()
                        .setStatus(Status.CREATED)
                        .build();
                }
                return new KeyValue<>(categoryKey, processedCategory);
            })
            .to(CATEGORIES_TOPIC, Produced.with(specificProto(), specificProto()));
    }

    private KafkaProtobufSerde<Category> specificProto() {

        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);

        final KafkaProtobufSerde<Category> kafkaProtobufSerde = new KafkaProtobufSerde<>(Category.class);
        kafkaProtobufSerde.configure(config, false);
        return kafkaProtobufSerde;
    }

}

@Component
class TestCategoryProducer {

  private final KafkaTemplate<Category, Category> kafkaTemplate;

  @Autowired
  TestCategoryProducer(KafkaTemplate<Category, Category> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @EventListener(ApplicationStartedEvent.class)
  public void produceMovies() {
    final Category message1= Category.newBuilder().setName("Postres").setParent("").build();
    final Category message2 = Category.newBuilder().setName("Dulces").setParent("").build();
    final Category message3 = Category.newBuilder().setName("Tortas").setParent("Postres").build();
    final Category message4 = Category.newBuilder().setName("Limpieza").setParent("").build();

    Stream.of(message1, message2, message3, message4).forEach(category -> kafkaTemplate.send("in-categories", category));

  }
}

@Component
class Consumer {

  private static final Logger log = LoggerFactory.getLogger(Consumer.class);

  @KafkaListener(topics = { "categories" }, groupId = "categories_listener")
  public void consume(ConsumerRecord<Category, Category> record) {
    log.info(record.value().toString());
  }
}