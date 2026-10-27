package pe.edu.vallegrande.bigdata.api.rest;

import pe.edu.vallegrande.bigdata.api.constant.ApiPaths;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pe.edu.vallegrande.bigdata.api.dto.response.JobResponse;
import pe.edu.vallegrande.bigdata.api.service.JobService;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping(ApiPaths.JOBS)
@RequiredArgsConstructor
public class JobRest {

    private final JobService jobService;

    @GetMapping
    public Flux<JobResponse> findAll() {
        return Flux.fromIterable(jobService.findAll());
    }

    @GetMapping(ApiPaths.BY_TYPE)
    public Mono<JobResponse> findByType(@PathVariable String type) {
        return Mono.fromSupplier(() -> jobService.findByType(type));
    }

}
