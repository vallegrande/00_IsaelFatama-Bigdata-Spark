package pe.edu.vallegrande.bigdata.api.service;

import pe.edu.vallegrande.bigdata.api.dto.response.JobResponse;
import pe.edu.vallegrande.bigdata.contracts.JobType;

import java.util.List;

public interface JobService {

    List<JobResponse> findAll();

    JobResponse findByType(String type);

    JobType resolve(String type);

}
