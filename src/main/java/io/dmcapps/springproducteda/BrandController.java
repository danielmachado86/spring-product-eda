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

import io.dmcapps.proto.catalog.Brand;
import io.dmcapps.proto.catalog.Brand.Status;

@RestController
@CrossOrigin
public class BrandController {

    private static final Logger log = LoggerFactory.getLogger(BrandController.class);


    @Autowired
    BrandStreamManager brandStreamManager;

    @PostMapping("/brands")
    public @ResponseBody ResponseEntity<Brand> addBrand(@RequestBody Brand productRequest) {
        io.dmcapps.proto.catalog.Brand.Builder brandBuilder = productRequest.toBuilder();
        Brand brand = brandBuilder.setStatus(Status.PENDING).build();
        brandStreamManager.produce(brand.getName(), brand);
        log.info("POST Request: {}", brand);
        return new ResponseEntity<>(brand, HttpStatus.OK);
    }
    
    @PutMapping("/brands/{name}")
    public @ResponseBody ResponseEntity<Brand> updateBrand(@RequestBody Brand brandRequest, @PathVariable String name) {
        io.dmcapps.proto.catalog.Brand.Builder brandBuilder = brandRequest.toBuilder();
        Brand brand = brandBuilder.setName(name).setStatus(Status.PENDING).build();
        brandStreamManager.produce(brand.getName(), brand);
        log.info("PUT Request: {}", brand);
        return new ResponseEntity<>(brand, HttpStatus.OK);
    }
    
    @DeleteMapping("/brands/{name}")
    public @ResponseBody ResponseEntity<Brand> deleteBrand(@PathVariable String name) {
        io.dmcapps.proto.catalog.Brand.Builder brandBuilder = io.dmcapps.proto.catalog.Brand.newBuilder();
        Brand brand = brandBuilder.setName(name).setStatus(Status.PENDING).build();
        brandStreamManager.produce(name);
        log.info("DELETE Request: {}", name);
        return new ResponseEntity<>(brand, HttpStatus.OK);
    }

}
