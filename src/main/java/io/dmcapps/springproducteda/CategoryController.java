package io.dmcapps.springproducteda;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.dmcapps.proto.catalog.Category;
import io.dmcapps.proto.catalog.Category.Status;

@RestController
@CrossOrigin
public class CategoryController {

    @Autowired
    CategoryStreamManager categoryStreamManager;
  
    @PostMapping("/categories")
    public Category addCategory(@RequestBody Category brandRequest) {
        Category category = brandRequest.toBuilder().setStatus(Status.PENDING).build();
        categoryStreamManager.produce(category);

        return category;
    }
}
