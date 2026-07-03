package com.foogaro.kinexis.core.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.Repository;
import org.springframework.stereotype.Component;

import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Component responsible for finding and managing Spring beans, particularly repositories and services.
 * This class provides functionality to locate and work with repositories and services in the application context,
 * handling both proxy and non-proxy instances, and supporting various repository operations.
 */
@Component
public class BeanFinder {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final Map<String, ?> allBeans;

    /**
     * Constructs a new BeanFinder with the specified ListableBeanFactory.
     * Initializes the bean finder by loading all beans from the application context.
     *
     * @param listableBeanFactory the Spring bean factory to use for bean lookup
     */
    public BeanFinder(ListableBeanFactory listableBeanFactory) {
        this.allBeans = listableBeanFactory.getBeansOfType(Object.class);
        logger.debug("Initialized BeanFinder with {} beans", allBeans.size());
    }

    /**
     * Finds all repositories that match a specific repository class name.
     * Handles both proxy and non-proxy repository instances.
     *
     * @param repositoryClassName the name of the repository class to find
     * @param <T> the type of entity the repository handles
     * @return a list of matching repositories
     */
    @SuppressWarnings("unchecked")
    public <T> List<Repository<T, ?>> findRepositoriesForEntity(String repositoryClassName) {
        List<Repository<T, ?>> repositories = allBeans.values()
                .stream()
                .filter(bean -> {
                    Class<?> beanClass = bean.getClass();
                    boolean isProxy = beanClass.getName().contains("$Proxy") || 
                                    beanClass.getName().contains("$JdkDynamicAopProxy");
                    
                    Class<?> actualClass = isProxy ? 
                        Arrays.stream(beanClass.getInterfaces())
                            .filter(i -> i.getSimpleName().equals(repositoryClassName))
                            .findFirst()
                            .orElse(beanClass) : 
                        beanClass;
                    
                    boolean nameMatches = actualClass.getSimpleName().equals(repositoryClassName);
                    boolean isRepository = Repository.class.isAssignableFrom(actualClass);
                    logger.trace("Bean: {} - Is proxy: {}, Actual class: {}, Name matches: {}, Is repository: {}",
                        beanClass.getName(), isProxy, actualClass.getName(), nameMatches, isRepository);
                    return nameMatches && isRepository;
                })
                .map(bean -> (Repository<T, ?>) bean)
                .collect(Collectors.toList());
        return repositories;
    }

    @SuppressWarnings("unchecked")
    public <T> List<Repository<T, ?>> findRepositoriesForEntity(Class<T> entityType) {
        return allBeans.values()
                .stream()
                .filter(bean -> repositoryEntityType(bean)
                        .map(type -> type.equals(entityType))
                        .orElse(false))
                .map(bean -> (Repository<T, ?>) bean)
                .collect(Collectors.toList());
    }

    private Optional<Class<?>> repositoryEntityType(Object bean) {
        for (Type genericInterface : bean.getClass().getGenericInterfaces()) {
            Optional<Class<?>> entityType = repositoryEntityType(genericInterface);
            if (entityType.isPresent()) {
                return entityType;
            }
        }
        for (Class<?> repositoryInterface : bean.getClass().getInterfaces()) {
            if (!Repository.class.isAssignableFrom(repositoryInterface)) {
                continue;
            }
            for (Type genericInterface : repositoryInterface.getGenericInterfaces()) {
                Optional<Class<?>> entityType = repositoryEntityType(genericInterface);
                if (entityType.isPresent()) {
                    return entityType;
                }
            }
        }
        return Optional.empty();
    }

    private Optional<Class<?>> repositoryEntityType(Type genericInterface) {
        if (!(genericInterface instanceof ParameterizedType parameterizedType)) {
            return Optional.empty();
        }
        if (!(parameterizedType.getRawType() instanceof Class<?> rawType)
                || !Repository.class.isAssignableFrom(rawType)) {
            return Optional.empty();
        }
        Type entityType = parameterizedType.getActualTypeArguments()[0];
        if (entityType instanceof Class<?> entityClass) {
            return Optional.of(entityClass);
        }
        return Optional.empty();
    }

    /**
     * Determines the ID type of a repository by analyzing its generic type parameters.
     *
     * @param repository the repository to analyze
     * @param <T> the type of entity the repository handles
     * @return the class representing the ID type, or null if not found
     */
    public <T> Class<?> getIdType(Repository<T, ?> repository) {
        return Arrays.stream(repository.getClass().getInterfaces())
                .filter(i -> Repository.class.isAssignableFrom(i))
                .filter(i -> i.getGenericInterfaces().length > 0)
                .map(i -> i.getGenericInterfaces()[0])
                .filter(type -> type instanceof ParameterizedType)
                .map(type -> (ParameterizedType) type)
                .map(paramType -> paramType.getActualTypeArguments()[1])
                .filter(type -> type instanceof Class)
                .map(type -> (Class<?>) type)
                .findFirst()
                .orElse(null);
    }

    /**
     * Creates an ID instance of the specified type from a string value.
     * Supports common ID types like String, UUID, and Number types.
     *
     * @param idType the class of the ID type
     * @param value the string value to convert to an ID
     * @return the created ID instance
     * @throws IllegalArgumentException if the ID cannot be created
     */
    public Object createId(Class<?> idType, String value) {
        Objects.requireNonNull(idType, "ID type cannot be null");
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("ID value cannot be null or empty");
        }
        try {
            if (idType == String.class) return value;
            if (idType == UUID.class) return UUID.fromString(value);
            if (Number.class.isAssignableFrom(idType)) {
                if (idType == Integer.class) return Integer.valueOf(value);
                if (idType == Long.class) return Long.valueOf(value);
            }
            Constructor<?> constructor = idType.getConstructor(String.class);
            return constructor.newInstance(value);
        } catch (Exception e) {
            logger.error("Failed to create ID of type {} with value {}", idType, value, e);
            throw new IllegalArgumentException("Cannot create ID", e);
        }
    }

    /**
     * Executes an operation on a repository using an ID value.
     * Converts the string ID to the appropriate type before execution.
     *
     * @param repository the repository to operate on
     * @param idValue the string ID value
     * @param operation the operation to execute
     * @param <T> the type of entity the repository handles
     * @param <ID> the type of ID the repository uses
     */
    public <T, ID> void executeIdOperation(Repository<T, ?> repository, String idValue,
                                           BiConsumer<CrudRepository<T, ID>, ID> operation) {
        Class<?> idType = getIdType(repository);
        @SuppressWarnings("unchecked")
        ID id = (ID) createId(idType, idValue);

        CrudRepository<T, ID> crudRepo = asCrudRepository(repository);
        operation.accept(crudRepo, id);
    }

    /**
     * Converts a Repository to a CrudRepository if possible.
     *
     * @param repository the repository to convert
     * @param <T> the type of entity the repository handles
     * @param <ID> the type of ID the repository uses
     * @return the repository as a CrudRepository
     * @throws IllegalArgumentException if the repository cannot be converted
     */
    @SuppressWarnings("unchecked")
    public <T, ID> CrudRepository<T, ID> asCrudRepository(Repository<T, ?> repository) {
        if (!(repository instanceof CrudRepository)) {
            throw new IllegalArgumentException("Repository must implement CrudRepository");
        }
        return (CrudRepository<T, ID>) repository;
    }
}
