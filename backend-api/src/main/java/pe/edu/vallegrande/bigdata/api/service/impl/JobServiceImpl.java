package pe.edu.vallegrande.bigdata.api.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import pe.edu.vallegrande.bigdata.api.dto.response.JobResponse;
import pe.edu.vallegrande.bigdata.api.exception.ResourceNotFoundException;
import pe.edu.vallegrande.bigdata.api.mapper.JobMapper;
import pe.edu.vallegrande.bigdata.api.service.JobService;
import pe.edu.vallegrande.bigdata.contracts.JobType;

import java.util.List;
import java.util.Locale;

@Service
@RequiredArgsConstructor
public class JobServiceImpl implements JobService {

    private final JobMapper jobMapper;

    @Override
    public List<JobResponse> findAll() {
        return List.of(JobType.values()).stream().map(jobMapper::toResponse).toList();
    }

    @Override
    public JobResponse findByType(String type) {
        return jobMapper.toResponse(resolve(type));
    }

    @Override
    public JobType resolve(String type) {
        if (type == null || type.isBlank()) {
            throw new ResourceNotFoundException("Falta el tipo de job");
        }
        String normalized = type.trim().toUpperCase(Locale.ROOT).replace('-', '_');
        for (JobType candidate : JobType.values()) {
            if (candidate.name().equals(normalized)) {
                return candidate;
            }
        }
        throw new ResourceNotFoundException("No existe el job '" + type + "'");
    }

}
