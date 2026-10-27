package pe.edu.vallegrande.bigdata.api.rest;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pe.edu.vallegrande.bigdata.api.constant.ApiPaths;
import pe.edu.vallegrande.bigdata.api.service.DatasetService;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;

@RestController
@RequestMapping(ApiPaths.DATASETS)
@RequiredArgsConstructor
public class DatasetRest {

    private final DatasetService datasetService;

    @GetMapping
    public Mono<List<String>> findAll() {
        return Mono.fromCallable(datasetService::findAll).subscribeOn(Schedulers.boundedElastic());
    }

}
