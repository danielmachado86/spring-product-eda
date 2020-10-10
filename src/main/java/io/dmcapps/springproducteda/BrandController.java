package io.dmcapps.springproducteda;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.dmcapps.proto.Brand;
import io.dmcapps.proto.Brand.Status;;

@RestController
@CrossOrigin
public class BrandController {

    @Autowired
    BrandStreamManager brandStreamManager;
  
    @PostMapping("/brands")
    public Brand addBrand(@RequestBody Brand brandRequest) {
        // System.out.println(productRequest);
        Brand brand = brandRequest.toBuilder().setStatus(Status.PENDING).build();
        brandStreamManager.produce(brand);

        return brand;
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
