package io.dmcapps.springproducteda;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import io.dmcapps.proto.catalog.Product;
import io.dmcapps.proto.catalog.Product.Status;

@RestController
@CrossOrigin
public class ProductController {

    private static final Logger log = LoggerFactory.getLogger(ProductController.class);


    @Autowired
    ProductStreamManager productStreamManager;

    @PostMapping("/products")
    public @ResponseBody ResponseEntity<Product> addProduct(@RequestBody Product productRequest) {
        io.dmcapps.proto.catalog.Product.Builder productBuilder = productRequest.toBuilder();
        if (!productRequest.getId().isEmpty()) {
            return new ResponseEntity<>(productBuilder.setStatus(Status.REJECTED).build(), HttpStatus.BAD_REQUEST);
        }
        Product product = productBuilder.setStatus(Status.PENDING).build();
        productStreamManager.produce(product.getId(), product);
        log.info("POST Request: {}", product);
        return new ResponseEntity<>(product, HttpStatus.OK);
    }
    
    @PutMapping("/products/{id}")
    public @ResponseBody ResponseEntity<Product> updateProduct(@RequestBody Product productRequest, @PathVariable String id) {
        io.dmcapps.proto.catalog.Product.Builder productBuilder = productRequest.toBuilder();
        Product product = productBuilder.setId(id).setStatus(Status.PENDING).build();
        productStreamManager.produce(product.getId(), product);
        log.info("PUT Request: {}", product);
        return new ResponseEntity<>(product, HttpStatus.OK);
    }
    
    @DeleteMapping("/products/{id}")
    public @ResponseBody ResponseEntity<Product> deleteProduct(@PathVariable String id) {
        io.dmcapps.proto.catalog.Product.Builder productBuilder = io.dmcapps.proto.catalog.Product.newBuilder();
        Product product = productBuilder.setId(id).setStatus(Status.PENDING).build();
        productStreamManager.produce(id);
        log.info("DELETE Request: {}", id);
        return new ResponseEntity<>(product, HttpStatus.OK);
    }

}
