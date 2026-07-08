package com.foogaro.kinexis.core.service;

import com.foogaro.kinexis.core.model.KinexisEventEnvelope;

public interface KinexisEventUpcaster {

    boolean supports(String entityType, String fromVersion, String toVersion);

    KinexisEventEnvelope upcast(KinexisEventEnvelope envelope, String toVersion);
}
