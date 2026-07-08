package com.foogaro.kinexis.core.service;

import com.foogaro.kinexis.core.model.KinexisEvent;
import com.foogaro.kinexis.core.model.KinexisEventEnvelope;

public interface KinexisEventSchemaRegistry {

    String currentVersion(String entityType);

    KinexisEventEnvelope upcast(KinexisEventEnvelope envelope);

    static KinexisEventSchemaRegistry noop() {
        return new KinexisEventSchemaRegistry() {
            @Override
            public String currentVersion(String entityType) {
                return KinexisEvent.CURRENT_SCHEMA_VERSION;
            }

            @Override
            public KinexisEventEnvelope upcast(KinexisEventEnvelope envelope) {
                return envelope;
            }
        };
    }
}
