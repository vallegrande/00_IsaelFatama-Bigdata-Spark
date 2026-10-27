package pe.edu.vallegrande.bigdata.api.mapper;

import org.springframework.stereotype.Component;
import pe.edu.vallegrande.bigdata.api.dto.response.JobResponse;
import pe.edu.vallegrande.bigdata.contracts.JobType;

@Component
public class JobMapper {

    public JobResponse toResponse(JobType type) {
        return new JobResponse(
                type.name(),
                type.label(),
                type.kind().name(),
                type.kind().label(),
                type.kind().input().name(),
                type.module(),
                type.kind().defaultThreshold(),
                type.goldTables());
    }

}
