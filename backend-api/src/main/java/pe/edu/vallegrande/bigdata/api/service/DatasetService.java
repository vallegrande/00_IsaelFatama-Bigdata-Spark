package pe.edu.vallegrande.bigdata.api.service;

import java.nio.file.Path;
import java.util.List;

public interface DatasetService {

    List<String> findAll();

    Path resolve(String name);

}
