package io.dmcapps.springproducteda;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
import io.dmcapps.proto.catalog.Brand;
import io.dmcapps.proto.catalog.Category;
import io.dmcapps.proto.catalog.Product;
import io.dmcapps.proto.catalog.Product.Builder;
import io.dmcapps.proto.catalog.Product.Status;

@Component
class ProductService {

    private static final Logger log = LoggerFactory.getLogger(ProductService.class);

    private static final String INPUT_PRODUCTS_TOPIC = "in-products";
    private static final String PRODUCTS_TOPIC = "products";
    private static final String INPUT_BRANDS_TOPIC = "in-brands";
    private static final String BRANDS_TOPIC = "brands";
    private static final String INPUT_CATEGORIES_TOPIC = "in-categories";
    private static final String CATEGORIES_TOPIC = "categories";

    @Value("${spring.kafka.properties.schema.registry.url}")
    String srUrl;

    @Value("${spring.kafka.properties.basic.auth.credentials.source}")
    String srCredentialSource;

    @Value("${spring.kafka.properties.schema.registry.basic.auth.user.info}")
    String srAuthUserInfo;

    @Bean
    public NewTopic inputCategoriesTopic() {
        return new NewTopic(INPUT_CATEGORIES_TOPIC, 1, (short) 3);
    }

    @Bean
    public NewTopic categoriesTopic() {
        return new NewTopic(CATEGORIES_TOPIC, 1, (short) 3);
    }

    @Bean
    public NewTopic inputBrandsTopic() {
        return new NewTopic(INPUT_BRANDS_TOPIC, 1, (short) 3);
    }

    @Bean
    public NewTopic brandsTopic() {
        return new NewTopic(BRANDS_TOPIC, 1, (short) 3);
    }

    @Bean
    public NewTopic inputProductsTopic() {
        return new NewTopic(INPUT_PRODUCTS_TOPIC, 1, (short) 3);
    }

    @Bean
    public NewTopic productsTopic() {
        return new NewTopic(PRODUCTS_TOPIC, 1, (short) 3);
    }

    @Autowired
    public void process(StreamsBuilder builder, StreamsBuilderFactoryBean streamsBuilderFB) {
        KafkaProtobufSerde<Category> specificCategoryProto = specificCategoryProto();
        KafkaProtobufSerde<Brand> specificBrandProto = specificBrandProto();
        KafkaProtobufSerde<Product> specificProductProto = specificProductProto();

        KTable<String, Category> categoriesTable = builder
            .table(CATEGORIES_TOPIC, Consumed.with(Serdes.String(), specificCategoryProto));
        
        KTable<String, Brand> brandsTable = builder
            .table(BRANDS_TOPIC, Consumed.with(Serdes.String(), specificBrandProto));
        
        KTable<String, Product> productsTable = builder
            .table(PRODUCTS_TOPIC, Consumed.with(Serdes.String(), specificProductProto));
            
        builder
            .stream(INPUT_CATEGORIES_TOPIC, Consumed.with(Serdes.String(), specificCategoryProto))
            .selectKey(
                (key, value) -> value.getName() + ":" + value.getParent()
            )
            .leftJoin(categoriesTable, (newCategory, existingCategory) -> {
                if (existingCategory == null){
                    return newCategory;
                }
                return existingCategory;
            })
            .filter((key, value) -> value.getStatus() == io.dmcapps.proto.catalog.Category.Status.PENDING)
            .mapValues(category -> {
                io.dmcapps.proto.catalog.Category.Builder categoryBuilder = category.toBuilder();  

                categoryBuilder.setStatus(io.dmcapps.proto.catalog.Category.Status.CREATED);
                return categoryBuilder.build();
            })
            .to(CATEGORIES_TOPIC, Produced.with(Serdes.String(), specificCategoryProto));
        
        
        builder
            .stream(INPUT_BRANDS_TOPIC, Consumed.with(Serdes.String(), specificBrandProto))
            .selectKey((key, value) -> value.getName())
            .leftJoin(brandsTable, (newBrand, existingBrand) -> {
                if (existingBrand == null){
                    return newBrand;
                }
                return existingBrand;
            })
            .filter((key, value) -> value.getStatus() == io.dmcapps.proto.catalog.Brand.Status.PENDING)
            .mapValues(brand -> {
                
                io.dmcapps.proto.catalog.Brand.Builder brandBuilder = brand.toBuilder();  
                
                brandBuilder.setStatus(io.dmcapps.proto.catalog.Brand.Status.CREATED);
                
                return brandBuilder.build();
            })
            .to(BRANDS_TOPIC, Produced.with(Serdes.String(), specificBrandProto));
        

        KStream<String, Product> inputStream = builder
            .stream(INPUT_PRODUCTS_TOPIC, Consumed.with(Serdes.String(), specificProductProto));

            KStream<String, Product> inputProducts = inputStream
            .filter((key, value) -> value != null)
            .selectKey((key, value) -> value.getBrand().getName())
            .join(brandsTable, (product, brand) -> 
                product.toBuilder().setBrand(brand).build()
            )
            .selectKey((key, value) -> value.getCategory().getName() + ":" + value.getCategory().getParent())
            .join(categoriesTable, (product, category) ->
                product.toBuilder().setCategory(category).build()
            );
            
        KStream<String, Product> updateProducts = inputProducts
            .selectKey(
                (key, value) -> value.getId()
            )
            .leftJoin(productsTable, (newProduct, existingProduct) -> {
                if (existingProduct == null){
                    return newProduct.toBuilder().setStatus(Status.REJECTED).build();
                }
                return newProduct;
            })
            .filter((key, value) -> value.getStatus() == Status.PENDING)
            .mapValues(product -> {
                log.info("Update: ");
                log.info(product.toString());
                Builder productBuilder = product.toBuilder();  
                
                productBuilder.setStatus(Status.UPDATED);
                
                return productBuilder.build();
                
            });
            
        KStream<String, Product> createProducts = inputProducts
            .selectKey(
                (key, value) -> {

                    String seed = value.getName() + ":" + value.getBrand().getName();
                    
                    return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
                    
                }
            )
            .leftJoin(productsTable, (newProduct, existingProduct) -> {
                if (existingProduct == null){
                    return newProduct;
                }
                return newProduct.toBuilder().setStatus(Status.REJECTED).build();
            })
            .filter((key, value) -> value.getStatus() == Status.PENDING)
            .mapValues(product -> {
                log.info("Create: ");
                log.info(product.toString());
                Builder productBuilder = product.toBuilder();  

                String seed = product.getName() + ":" + product.getBrand().getName();
                final String uuid = UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();

                productBuilder.setId(uuid).setStatus(Status.CREATED);

                return productBuilder.build();

            });
            
        KStream<String, Product> deleteProducts = inputStream
            .filter((key, value) -> value == null && key != null);

        createProducts.to(PRODUCTS_TOPIC, Produced.with(Serdes.String(), specificProductProto));
        updateProducts.to(PRODUCTS_TOPIC, Produced.with(Serdes.String(), specificProductProto));
        deleteProducts.to(PRODUCTS_TOPIC, Produced.with(Serdes.String(), specificProductProto));
    }

    private KafkaProtobufSerde<Product> specificProductProto() {

        final Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        config.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, srCredentialSource);
        config.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, srAuthUserInfo);

        final KafkaProtobufSerde<Product> kafkaProtobufSerde = new KafkaProtobufSerde<>(Product.class);
        kafkaProtobufSerde.configure(config, false);
        // kafkaProtobufSerde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, srCredentialSource), false);
        // kafkaProtobufSerde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, srAuthUserInfo), false);
        return kafkaProtobufSerde;
    }

    private KafkaProtobufSerde<Brand> specificBrandProto() {

        final Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        config.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, srCredentialSource);
        config.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, srAuthUserInfo);

        final KafkaProtobufSerde<Brand> kafkaProtobufSerde = new KafkaProtobufSerde<>(Brand.class);
        kafkaProtobufSerde.configure(config, false);
        return kafkaProtobufSerde;
    }

    private KafkaProtobufSerde<Category> specificCategoryProto() {

        final Map<String, String> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUrl);
        config.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, srCredentialSource);
        config.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, srAuthUserInfo);

        final KafkaProtobufSerde<Category> kafkaProtobufSerde = new KafkaProtobufSerde<>(Category.class);
        kafkaProtobufSerde.configure(config, false);
        return kafkaProtobufSerde;
    }

}