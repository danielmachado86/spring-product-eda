package io.dmcapps.springproducteda;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.dmcapps.proto.catalog.Product;
import io.dmcapps.proto.catalog.Product.Status;
import io.dmcapps.proto.catalog.ProductResponse;
import io.dmcapps.proto.catalog.ProductResponse.Builder;

@RestController()
@CrossOrigin
public class ProductController {

    private static final Logger log = LoggerFactory.getLogger(ProductController.class);


    @Autowired
    ProductStreamManager productStreamManager;

    @PostMapping("/products")
    public Product addProduct(@RequestBody Product productRequest) {
        Product product = productRequest.toBuilder().setStatus(Status.PENDING).build();
        productStreamManager.produce(product);
        log.info("Request: {}", product);
        return product;
    }
    
    @GetMapping(value={"/products"}, produces = MediaType.APPLICATION_JSON_VALUE)
    public ProductResponse getAllProducts() {
        
        ReadOnlyKeyValueStore<String, Product> productStore = productStreamManager.getProductStore();
        
        Builder allProducts = ProductResponse.newBuilder();

        try(KeyValueIterator<String, Product> itr = productStore.all()) {
            while (itr.hasNext()){
                allProducts.addProducts(itr.next().value);
            }
        }
        log.info("Response: {}", allProducts);
        return allProducts.build();
    }
    
    @GetMapping(value={"search/products/{q}"}, produces = MediaType.APPLICATION_JSON_VALUE)
    public ProductResponse searchProducts(@PathVariable("q") String q) {
        
        log.info("Search query: {}", q);
        
        ReadOnlyKeyValueStore<String, Product> productStore = productStreamManager.getProductStore();
        
        Builder allProducts = ProductResponse.newBuilder();

        try(KeyValueIterator<String, Product> itr = productStore.all()) {
            while (itr.hasNext()){
                allProducts.addProducts(itr.next().value);
            }
        }
        log.info("Response: {}", allProducts);
        return allProducts.build();
    }

}
