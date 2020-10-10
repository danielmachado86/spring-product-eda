package io.dmcapps.springproducteda;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.dmcapps.proto.Category;
import io.dmcapps.proto.Category.Status;;

@RestController
@CrossOrigin
public class CategoryController {

    @Autowired
    CategoryStreamManager categoryStreamManager;
  
    @PostMapping("/categories")
    public Category addCategory(@RequestBody Category brandRequest) {
        // System.out.println(productRequest);
        Category category = brandRequest.toBuilder().setStatus(Status.PENDING).build();
        categoryStreamManager.produce(category);

        return category;
    }
    // @GetMapping("/products")
    // public Product getAllProducts(@PathVariable String id) {
    //     // System.out.println(productRequest);
    //     Product product = productRequest.toBuilder().setStatus(Status.PENDING).build();
    //     productStreamManager.produce(product);

    //     return product;
    // }
    // @GetMapping("/products/{id}")
    // public Product getProduct(@PathVariable String id) {
    //     // System.out.println(productRequest);
    //     Product product = productRequest.toBuilder().setStatus(Status.PENDING).build();
    //     productStreamManager.produce(product);

    //     return product;
    // }
}
