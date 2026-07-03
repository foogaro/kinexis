package com.foogaro.kinexis.core.stream;

import com.foogaro.kinexis.core.model.KinexisEvent;

public interface EventPublisher {

    String append(Class<?> entityType, KinexisEvent event);
}
