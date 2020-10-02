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
import io.dmcapps.proto.Brand;
import io.dmcapps.proto.Brand.Status;


@Component
class BrandService {

    private static final Logger log = LoggerFactory.getLogger(BrandService.class);

    private static final String INPUT_BRANDS_TOPIC = "in-brands";
    private static final String BRANDS_TOPIC = "brands";
    private static final String BRANDS_STORE = "brands-store";


    @Value("${spring.kafka.properties.schema.registry.url}")
    String srUrl;

    @Bean
    public NewTopic inputBrandsTopic() {
        return new NewTopic(INPUT_BRANDS_TOPIC, 3, (short) 1);
    }

    @Bean
    public NewTopic BrandsTopic() {
        return new NewTopic(BRANDS_TOPIC, 3, (short) 1);
    }

    @Autowired
    public void process(StreamsBuilder builder, StreamsBuilderFactoryBean streamsBuilderFB) {
        builder
            .stream(INPUT_BRANDS_TOPIC, Consumed.with(specificProto(), specificProto()))
            .map((key, brand) -> {
                StoreQueryParameters<ReadOnlyKeyValueStore<Brand, Brand>> sqp = StoreQueryParameters
                    .fromNameAndType(BRANDS_STORE, QueryableStoreTypes.keyValueStore());

                Brand brandKey = Brand.newBuilder().setName(brand.getName()).build();

                Brand storedBrand = streamsBuilderFB
                    .getKafkaStreams()
                    .store(sqp)
                    .get(brandKey);
                log.info(brand.toString());
                Brand processedBrand = null;
                if (storedBrand != null) {
                    if (storedBrand.getStatus() != Status.CREATED
                            || storedBrand.getStatus() != Status.UPDATED) {
                        processedBrand = brand
                            .toBuilder()
                            .setStatus(Status.UPDATED)
                            .build();
                    }
                } else {
                    processedBrand = brand
                        .toBuilder()
                        .setStatus(Status.CREATED)
                        .build();
                }
                return new KeyValue<>(brandKey, processedBrand);
            })
            .to(BRANDS_TOPIC, Produced.with(specificProto(), specificProto()));
    }

    private KafkaProtobufSerde<Brand> specificProto() {

        final Map<String, String> config = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);

        final KafkaProtobufSerde<Brand> kafkaProtobufSerde = new KafkaProtobufSerde<>(Brand.class);
        kafkaProtobufSerde.configure(config, false);
        return kafkaProtobufSerde;
    }

}

@Component
class TestBrandProducer {

  private final KafkaTemplate<Brand, Brand> kafkaTemplate;

  @Autowired
  TestBrandProducer(KafkaTemplate<Brand, Brand> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  @EventListener(ApplicationStartedEvent.class)
  public void produceMovies() {
    final Brand message1= Brand.newBuilder().setName("Ramo").build();
    final Brand message2 = Brand.newBuilder().setName("Noel").build();

    Stream.of(message1, message2).forEach(brand -> kafkaTemplate.send("in-brands", brand));

  }
}

@Component
class TestBrandConsumer {

  private static final Logger log = LoggerFactory.getLogger(Consumer.class);

  @KafkaListener(topics = { "brands" }, groupId = "brands_listener")
  public void consume(ConsumerRecord<Brand, Brand> record) {
    log.info(record.value().toString());
  }
}