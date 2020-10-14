package io.dmcapps.springproducteda;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.dmcapps.proto.catalog.Brand;
import io.dmcapps.proto.catalog.Brand.Status;

@RestController
@CrossOrigin
public class BrandController {

    @Autowired
    BrandStreamManager brandStreamManager;
  
    @PostMapping("/brands")
    public Brand addBrand(@RequestBody Brand brandRequest) {
        Brand brand = brandRequest.toBuilder().setStatus(Status.PENDING).build();
        brandStreamManager.produce(brand);

        return brand;
    }
}
