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

import io.dmcapps.proto.catalog.Category;
import io.dmcapps.proto.catalog.Category.Status;

@RestController
@CrossOrigin
public class CategoryController {

    private static final Logger log = LoggerFactory.getLogger(CategoryController.class);

    @Autowired
    CategoryStreamManager categoryStreamManager;

    @PostMapping("/categories")
    public @ResponseBody ResponseEntity<Category> addCategory(@RequestBody Category categoryRequest) {
        io.dmcapps.proto.catalog.Category.Builder categoryBuilder = categoryRequest.toBuilder();
        Category category = categoryBuilder.setStatus(Status.PENDING).build();
        categoryStreamManager.produce(category.getName(), category);
        log.info("POST Request: {}", category);
        return new ResponseEntity<>(category, HttpStatus.OK);
    }
    
    @PutMapping("/categories/parent/{parent}/name/{name}")
    public @ResponseBody ResponseEntity<Category> updateCategory(@RequestBody Category categoryRequest, @PathVariable String parent, @PathVariable String name) {
        io.dmcapps.proto.catalog.Category.Builder categoryBuilder = categoryRequest.toBuilder();
        Category category = categoryBuilder.setName(name).setParent(parent).setStatus(Status.PENDING).build();
        categoryStreamManager.produce(name + ":" + parent, category);
        log.info("PUT Request: {}", category);
        return new ResponseEntity<>(category, HttpStatus.OK);
    }
    
    @DeleteMapping("/categories/parent/{parent}/name/{name}")
    public @ResponseBody ResponseEntity<Category> deleteCategory(@PathVariable String parent, @PathVariable String name) {
        io.dmcapps.proto.catalog.Category.Builder categoryBuilder = io.dmcapps.proto.catalog.Category.newBuilder();
        Category category = categoryBuilder.setName(name).setParent(parent).setStatus(Status.PENDING).build();
        categoryStreamManager.produce(name + ":" + parent);
        log.info("DELETE Request: {}", name);
        return new ResponseEntity<>(category, HttpStatus.OK);
    }

}
