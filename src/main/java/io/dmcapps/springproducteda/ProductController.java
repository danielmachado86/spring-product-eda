package io.dmcapps.springproducteda;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.dmcapps.proto.Product;
import io.dmcapps.proto.ProductResponse;
import io.dmcapps.proto.Product.Status;
import io.dmcapps.proto.ProductResponse.Builder;;

@RestController
@CrossOrigin
public class ProductController {

    private static final Logger log = LoggerFactory.getLogger(ProductController.class);


    @Autowired
    ProductStreamManager productStreamManager;

    @PostMapping("/products")
    public Product addProduct(@RequestBody Product productRequest) {
        Product product = productRequest.toBuilder().setStatus(Status.PENDING).build();
        productStreamManager.produce(product);
        log.info("Request:\n" + product.toString());
        return product;
    }

    @GetMapping("/products")
    public ProductResponse getAllProducts() {

        ReadOnlyKeyValueStore<String, Product> productStore = productStreamManager.getProductStore();
        
        Builder allProducts = ProductResponse.newBuilder();

        try(KeyValueIterator<String, Product> itr = productStore.all()) {
            while (itr.hasNext()){
                allProducts.addProducts(itr.next().value);
            }
            itr.close();
        }
        System.out.println(allProducts.toString());
        return allProducts.build();
    }

    // @GetMapping("/products/{id}")
    // public Product getProduct(@PathVariable String id) {
    //     // System.out.println(productRequest);
    //     Product product = productRequest.toBuilder().setStatus(Status.PENDING).build();
    //     productStreamManager.produce(product);

    //     return product;
    // }
}
