// package io.dmcapps.springproducteda;

// import java.util.stream.Stream;

// import org.apache.kafka.clients.consumer.ConsumerRecord;
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.boot.context.event.ApplicationStartedEvent;
// import org.springframework.context.event.EventListener;
// import org.springframework.kafka.annotation.KafkaListener;
// import org.springframework.kafka.core.KafkaTemplate;
// import org.springframework.stereotype.Component;

// import io.dmcapps.proto.catalog.Category;
// import io.dmcapps.proto.catalog.Category.Status;



// @Component
// class CategoryStreamManager {

//     private static final String INPUT_CATEGORIES_TOPIC = "in-categories";


//     private final KafkaTemplate<String, Category> kafkaTemplate;

//     @Autowired
//     CategoryStreamManager(KafkaTemplate<String, Category> kafkaTemplate) {
//         this.kafkaTemplate = kafkaTemplate;
//     }


//     public void delete(String key) {
//       kafkaTemplate.send(INPUT_CATEGORIES_TOPIC, key, null);
//     }

//     public void produce(String key, Category category) {
//       kafkaTemplate.send(INPUT_CATEGORIES_TOPIC, key, category);
//     }

// }

// @Component
// class TestCategoryProducer {

//   private final KafkaTemplate<String, Category> kafkaTemplate;

//   @Autowired
//   TestCategoryProducer(KafkaTemplate<String, Category> kafkaTemplate) {
//     this.kafkaTemplate = kafkaTemplate;
//   }

//   @EventListener(ApplicationStartedEvent.class)
//   public void produceMovies() {
//     final Category message1= Category.newBuilder().setName("Postres").setParent("").setStatus(Status.PENDING).build();
//     final Category message2 = Category.newBuilder().setName("Dulces").setParent("").setStatus(Status.PENDING).build();
//     final Category message3 = Category.newBuilder().setName("Tortas").setParent("Postres").setStatus(Status.PENDING).build();
//     final Category message4 = Category.newBuilder().setName("Limpieza").setParent("").setStatus(Status.PENDING).build();

//     Stream.of(message1, message2, message3, message4).forEach(category -> kafkaTemplate.send("in-categories", category));

//   }
// }

// @Component
// class TestCategoryConsumer {

//   private static final Logger log = LoggerFactory.getLogger(TestCategoryConsumer.class);

//   @KafkaListener(topics = { "categories" }, groupId = "categories_listener")
//   public void consume(ConsumerRecord<String, Category> record) {
//     log.info("Category: {}", record.value());
//   }
// }