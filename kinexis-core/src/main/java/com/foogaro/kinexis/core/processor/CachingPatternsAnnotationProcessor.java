package com.foogaro.kinexis.core.processor;

import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.model.CachingFormat;
import com.foogaro.kinexis.core.model.CachingPattern;
import com.palantir.javapoet.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import javax.annotation.processing.*;
import javax.annotation.processing.AbstractProcessor;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Annotation processor for the {@link CachingPatterns} annotation.
 * This processor generates the necessary classes for implementing caching patterns
 * in Redis, including StreamListeners, Processors, ProcessOrchestrators,
 * PendingMessageHandlers, and KeyExpirationListeners.
 */
@SupportedAnnotationTypes("com.foogaro.kinexis.core.annotation.CachingPatterns")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
public class CachingPatternsAnnotationProcessor extends AbstractProcessor {

    private Types typeUtils;
    private Elements elementUtils;
    private Filer filer;
    private Messager messager;

    private final String basePackage = "com.foogaro.kinexis.core";

    /**
     * Default constructor for CachingPatternsAnnotationProcessor.
     * This constructor is used by the annotation processing framework to create instances
     * of this processor.
     */
    public CachingPatternsAnnotationProcessor() {
    }

    /**
     * Returns the latest supported source version.
     *
     * @return the latest supported source version
     */
    @Override
    public SourceVersion getSupportedSourceVersion() {
        return SourceVersion.latest();
    }

    /**
     * Initializes the processor with the processing environment.
     * Sets up utility classes for type processing, element processing,
     * file generation, and messaging.
     *
     * @param processingEnv the processing environment
     */
    @Override
    public synchronized void init(ProcessingEnvironment processingEnv) {
        super.init(processingEnv);
        this.typeUtils = processingEnv.getTypeUtils();
        this.elementUtils = processingEnv.getElementUtils();
        this.filer = processingEnv.getFiler();
        this.messager = processingEnv.getMessager();

        processingEnv.getMessager().printMessage(
                Diagnostic.Kind.NOTE,
                "Caching Patterns ⚞☲⚟ Initialized\n"
        );
    }

    /**
     * Processes the annotations and generates the necessary classes.
     * For each class annotated with {@link CachingPatterns}, this method:
     * 1. Finds all repositories managing the entity
     * 2. Generates StreamListener, Processor, ProcessOrchestrator, and PendingMessageHandler for each repository
     * 3. Generates KeyExpirationListener if the entity uses REFRESH_AHEAD pattern
     *
     * @param annotations the annotation types to process
     * @param roundEnv the current round environment
     * @return true if the annotations were processed successfully
     */
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        processingEnv.getMessager().printMessage(
                Diagnostic.Kind.NOTE,
                "Caching Patterns ⚞☲⚟ Processing\n"
        );

        Set<? extends Element> elements = roundEnv.getElementsAnnotatedWith(CachingPatterns.class);
        if (elements.isEmpty()) {
            warning("No entities found with annotation %s", CachingPatterns.class.getSimpleName());
            return false;
        }

        processingEnv.getMessager().printMessage(
                Diagnostic.Kind.NOTE,
                "Number of elements annotated with @CachingPatterns: " + elements.size()
        );

        try {
            for (Element element : elements) {
                if (element.getKind() != ElementKind.CLASS) {
                    error(element, "Only classes can be annotated with @CachingPatterns");
                    continue;
                }

                TypeElement entityElement = (TypeElement) element;
                String className = entityElement.getSimpleName().toString();
                String packageName = elementUtils.getPackageOf(entityElement).getQualifiedName().toString() + "." + className.toLowerCase();
                // Finds all repos managing the entity
                Set<TypeElement> repositories = findRepositoriesForEntity(roundEnv, entityElement);
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"Generating RedisRepository(s), StreamListener(s), Processors(s), ProcessOrchestrator(s) and PendingMessageHandler(s)");
                for (TypeElement repository : repositories) {
                    generateStreamListener(packageName, className, entityElement, TypeName.get(repository.asType()));
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tStreamListener for: " + repository);
                    generateProcessor(packageName, className, entityElement, TypeName.get(repository.asType()));
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tProcessor for: " + repository);
                    generateProcessOrchestrator(packageName, className, entityElement, TypeName.get(repository.asType()));
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tProcessOrchestrator for: " + repository);
                    generatePendingMessageHandler(packageName, className, entityElement, TypeName.get(repository.asType()));
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tPendingMessageHandler for: " + repository);
                }
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"Generating RedisRepository, RedisStreamListener, RedisProcessors, RedisProcessOrchestrator and RedisPendingMessageHandler");
                String newRedisRepository = generateRedisRepository(packageName, className, entityElement);
                assert newRedisRepository != null;
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tRedisRepository for: " + entityElement);
                generateStreamListener(packageName, className, entityElement, ClassName.bestGuess(newRedisRepository));
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tStreamListener for: " + newRedisRepository);
                generateProcessor(packageName, className, entityElement, ClassName.bestGuess(newRedisRepository));
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tProcessor for: " + newRedisRepository);
                generateProcessOrchestrator(packageName, className, entityElement, ClassName.bestGuess(newRedisRepository));
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tProcessOrchestrator for: " + newRedisRepository);
                generatePendingMessageHandler(packageName, className, entityElement, ClassName.bestGuess(newRedisRepository));
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tPendingMessageHandler for: " + newRedisRepository);

                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"Generating KeyExpirationListener(s)");
                Arrays.stream(element.getAnnotation(CachingPatterns.class).patterns())
                        .filter(pattern -> pattern.getValue() == CachingPattern.REFRESH_AHEAD.getValue())
                        .findFirst()
                        .ifPresent(pattern -> {
                            generateKeyExpirationListener(packageName, className, entityElement);
                            processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "\tKeyExpirationListener for: " + entityElement);
                        });
            }
        } catch (Exception e) {
            error(null, "Error processing @CachingPatterns annotation: %s", e.getMessage());
        }
        return true;
    }

    private TypeElement getTypeElementFromName(String className) {
        try {
            return elementUtils.getTypeElement(className);
        } catch (Exception e) {
            processingEnv.getMessager().printMessage(Diagnostic.Kind.WARNING,
                    "Could not find type element for: " + className);
            return null;
        }
    }

    /**
     * Finds all repositories that manage the given entity.
     * Searches for interfaces and classes annotated with {@link Repository}
     * that extend {@link CrudRepository} with the entity as its type parameter.
     *
     * @param roundEnv the current round environment
     * @param entityElement the entity element to find repositories for
     * @return a set of repository type elements
     */
    private Set<TypeElement> findRepositoriesForEntity(RoundEnvironment roundEnv, TypeElement entityElement) {
        Set<TypeElement> repositories = new HashSet<>();

        for (Element element : roundEnv.getElementsAnnotatedWith(Repository.class)) {
            if (element.getKind() != ElementKind.INTERFACE && element.getKind() != ElementKind.CLASS) {
                continue;
            }

            TypeElement repository = (TypeElement) element;
            if (isRepositoryForEntity(repository, entityElement)) {
                repositories.add(repository);
            }
        }

        return repositories;
    }

    /**
     * Checks if a repository manages a specific entity.
     * Verifies that the repository extends {@link CrudRepository} with the entity
     * as its first type parameter.
     *
     * @param repository the repository to check
     * @param entity the entity to check against
     * @return true if the repository manages the entity
     */
    private boolean isRepositoryForEntity(TypeElement repository, TypeElement entity) {
        for (TypeMirror superInterface : repository.getInterfaces()) {
            DeclaredType declaredType = (DeclaredType) superInterface;
            TypeElement interfaceElement = (TypeElement) declaredType.asElement();

            // Checks if it's a CrudRepository or one of its subclass
            if (implementsCrudRepository(interfaceElement)) {
                List<? extends TypeMirror> typeArguments = declaredType.getTypeArguments();
                if (!typeArguments.isEmpty() &&
                        typeUtils.isSameType(typeArguments.get(0), entity.asType())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Checks if a type implements {@link CrudRepository}.
     *
     * @param type the type to check
     * @return true if the type implements CrudRepository
     */
    private boolean implementsCrudRepository(TypeElement type) {
        TypeElement crudRepositoryType = elementUtils.getTypeElement(CrudRepository.class.getCanonicalName());
        return typeUtils.isAssignable(
                type.asType(),
                typeUtils.erasure(crudRepositoryType.asType())
        );
    }

    private String generateRedisRepository(String packageName, String className, TypeElement entityElement) {
        String repositoryClassName = className + "RedisRepository";

        // Find the @Id field and get its type
        TypeMirror idType = findIdFieldType(entityElement);
        if (idType == null) {
            error(entityElement, "Entity %s must have a field annotated with @Id.", className);
            return null;
        }

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get("com.redis.om.spring.repository", getRedisRepositorySuperInterface(entityElement)),
                TypeName.get(entityElement.asType()),
                TypeName.get(idType)
        );

        // Add @Repository annotation
        AnnotationSpec repositoryAnnotation = AnnotationSpec.builder(Repository.class).build();

        TypeSpec repositoryInterface = TypeSpec.interfaceBuilder(repositoryClassName)
                .addModifiers(Modifier.PUBLIC)
                .addSuperinterface(superclass)
                .addAnnotation(repositoryAnnotation)
                .build();

        // Write to the repository package
        String repositoryPackage = packageName + ".repository";
        writeJavaFile(repositoryPackage, repositoryInterface);
        return repositoryPackage + "." + repositoryClassName;
    }

    /**
     * Finds the type of the field annotated with @Id in the given entity.
     *
     * @param entityElement the entity type element to search in
     * @return the type mirror of the @Id field, or null if not found
     */
    private TypeMirror findIdFieldType(TypeElement entityElement) {
        TypeElement idAnnotation = elementUtils.getTypeElement("jakarta.persistence.Id");
        if (idAnnotation == null) {
            idAnnotation = elementUtils.getTypeElement("javax.persistence.Id");
        }
        if (idAnnotation == null) {
            error(entityElement, "Could not find @Id annotation type");
            return null;
        }

        for (Element enclosedElement : entityElement.getEnclosedElements()) {
            if (enclosedElement.getKind() == ElementKind.FIELD) {
                for (AnnotationMirror annotation : enclosedElement.getAnnotationMirrors()) {
                    if (typeUtils.isSameType(annotation.getAnnotationType(), idAnnotation.asType())) {
                        return ((VariableElement) enclosedElement).asType();
                    }
                }
            }
        }

        return null;
    }

    private String getRedisRepositorySuperInterface(TypeElement entityElement) {
        CachingPatterns cachingPatterns = entityElement.getAnnotation(CachingPatterns.class);
        CachingFormat format = cachingPatterns.format();
        if (format == CachingFormat.HASH) {
            return "RedisEnhancedRepository";
        } else if (format == CachingFormat.JSON) {
            return "RedisDocumentRepository";
        } else {
            throw new IllegalArgumentException("Unsupported format: " + format);
        }

    }

    /**
     * Generates a StreamListener class for the given entity and repository.
     * The generated class extends {@link com.foogaro.kinexis.core.listener.AbstractStreamListener} and includes
     * necessary fields and methods for handling Redis Stream messages.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param repository the repository type element
     */
    private void generateStreamListener(String packageName, String className,
                                        TypeElement entityElement, TypeName repository) {
        String listenerClassName = getRepositorySimpleName(repository) + "StreamListener";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".listener", "AbstractStreamListener"),
                TypeName.get(entityElement.asType()),
                repository
        );

        FieldSpec repositoryField = FieldSpec.builder(
                        repository,
                        getRepositoryInstanceName(repository),
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        FieldSpec redisTemplateField = FieldSpec.builder(
                        ParameterizedTypeName.get(
                                ClassName.get("org.springframework.data.redis.core", "RedisTemplate"),
                                ClassName.get(String.class),
                                ClassName.get(String.class)),
                        "redisTemplate",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        FieldSpec streamMessageListenerContainerField = FieldSpec.builder(
                        ParameterizedTypeName.get(
                                ClassName.get("org.springframework.data.redis.stream", "StreamMessageListenerContainer"),
                                ClassName.get(String.class),
                                ParameterizedTypeName.get(
                                        ClassName.get("org.springframework.data.redis.connection.stream", "MapRecord"),
                                        ClassName.get(String.class),
                                        ClassName.get(String.class),
                                        ClassName.get(String.class))),
                        "streamMessageListenerContainer",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        FieldSpec objectMapperField = FieldSpec.builder(
                        ClassName.get("com.fasterxml.jackson.databind", "ObjectMapper"),
                        "objectMapper",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        FieldSpec processOrchestratorField = FieldSpec.builder(
                        ClassName.get(packageName + ".orchestrator", getRepositorySimpleName(repository) + "ProcessOrchestrator"),
                        getRepositoryInstanceName(repository) + "ProcessOrchestrator",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        FieldSpec processorField = FieldSpec.builder(
                        ClassName.get(packageName + ".processor", getRepositorySimpleName(repository) + "Processor"),
                        getRepositoryInstanceName(repository) + "Processor",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        MethodSpec getRedisTemplateMethod = MethodSpec.methodBuilder("getRedisTemplate")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(redisTemplateField.type())
                .addStatement("return redisTemplate")
                .build();

        MethodSpec getStreamListenerContainerMethod = MethodSpec.methodBuilder("getStreamMessageListenerContainer")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(streamMessageListenerContainerField.type())
                .addStatement("return streamMessageListenerContainer")
                .build();

        MethodSpec getObjectMapperMethod = MethodSpec.methodBuilder("getObjectMapper")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(objectMapperField.type())
                .addStatement("return objectMapper")
                .build();

        MethodSpec deleteEntityMethod = MethodSpec.methodBuilder("deleteEntity")
                .addModifiers(Modifier.PROTECTED)
                .addParameter(Object.class, "id")
                .addStatement(getRepositoryInstanceName(repository) + ".deleteById(($T) id)", Long.class)
                .build();

        MethodSpec saveEntityMethod = MethodSpec.methodBuilder("saveEntity")
                .addModifiers(Modifier.PROTECTED)
                .addParameter(TypeName.get(entityElement.asType()), "entity")
                .returns(TypeName.get(entityElement.asType()))
                .addStatement("return " + getRepositoryInstanceName(repository) + ".save(entity)")
                .build();

        MethodSpec getProcessOrchestratorMethod = MethodSpec.methodBuilder("getProcessOrchestrator")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ClassName.get(packageName + ".orchestrator", getRepositorySimpleName(repository) + "ProcessOrchestrator"))
                .addStatement("return " + getRepositoryInstanceName(repository) + "ProcessOrchestrator")
                .build();

        MethodSpec getProcessorMethod = MethodSpec.methodBuilder("getProcessor")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ClassName.get(packageName + ".processor", getRepositorySimpleName(repository) + "Processor"))
                .addStatement("return " + getRepositoryInstanceName(repository) + "Processor")
                .build();

        TypeSpec streamListenerClass = TypeSpec.classBuilder(listenerClassName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(superclass)
                .addAnnotation(Component.class)
                .addField(repositoryField)
                .addField(redisTemplateField)
                .addField(streamMessageListenerContainerField)
                .addField(objectMapperField)
                .addField(processOrchestratorField)
                .addField(processorField)
                .addMethod(getRedisTemplateMethod)
                .addMethod(getStreamListenerContainerMethod)
                .addMethod(getObjectMapperMethod)
                .addMethod(deleteEntityMethod)
                .addMethod(saveEntityMethod)
                .addMethod(getProcessOrchestratorMethod)
                .addMethod(getProcessorMethod)
                .build();

        writeJavaFile(packageName + ".listener", streamListenerClass);
    }

    /**
     * Generates a Processor class for the given entity and repository.
     * The generated class implements the necessary processing logic for Redis Stream messages.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param repository the repository type element
     */
    private void generateProcessor(String packageName, String className,
                                   TypeElement entityElement, TypeName repository) {
        String processorClassName = getRepositorySimpleName(repository) + "Processor";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".processor", "AbstractProcessor"),
                TypeName.get(entityElement.asType()),
                repository
        );

        FieldSpec beanFactoryField = FieldSpec.builder(
                        ClassName.get("org.springframework.beans.factory", "ListableBeanFactory"),
                        "beanFactory",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        FieldSpec beanFinderField = FieldSpec.builder(
                        ClassName.get(basePackage + ".service", "BeanFinder"),
                        "beanFinder",
                        Modifier.PRIVATE)
                .build();

        MethodSpec getRepositoryFinderMethod = MethodSpec.methodBuilder("getRepositoryFinder")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ClassName.get(basePackage + ".service", "BeanFinder"))
                .beginControlFlow("if (beanFinder == null)")
                .addStatement("beanFinder = new $T(beanFactory)",
                        ClassName.get(basePackage + ".service", "BeanFinder"))
                .endControlFlow()
                .addStatement("return beanFinder")
                .build();

        TypeSpec processorClass = TypeSpec.classBuilder(processorClassName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(superclass)
                .addAnnotation(Component.class)
                .addField(beanFactoryField)
                .addField(beanFinderField)
                .addMethod(getRepositoryFinderMethod)
                .build();

        writeJavaFile(packageName + ".processor", processorClass);
    }

    /**
     * Generates a ProcessOrchestrator class for the given entity and repository.
     * The generated class coordinates the message processing flow.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param repository the repository type element
     */
    private void generateProcessOrchestrator(String packageName, String className,
                                             TypeElement entityElement, TypeName repository) {
        String orchestratorClassName = getRepositorySimpleName(repository) + "ProcessOrchestrator";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".orchestrator", "AbstractProcessOrchestrator"),
                TypeName.get(entityElement.asType()),
                repository
        );


        TypeSpec processOrchestratorClass = TypeSpec.classBuilder(orchestratorClassName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(superclass)
                .addAnnotation(Component.class)
                .build();

        writeJavaFile(packageName + ".orchestrator", processOrchestratorClass);
    }

    /**
     * Generates a PendingMessageHandler class for the given entity and repository.
     * The generated class handles messages that have been delivered but not acknowledged.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param repository the repository type element
     */
    private void generatePendingMessageHandler(String packageName, String className,
                                               TypeElement entityElement, TypeName repository) {
        String handlerClassName = getRepositorySimpleName(repository) + "PendingMessageHandler";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".handler", "AbstractPendingMessageHandler"),
                TypeName.get(entityElement.asType()),
                repository
        );

        FieldSpec processorField = FieldSpec.builder(
                        ClassName.get(packageName + ".processor", getRepositorySimpleName(repository) + "Processor"),
                        "processor",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        MethodSpec getProcessorMethod = MethodSpec.methodBuilder("getProcessor")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ClassName.get(packageName + ".processor", getRepositorySimpleName(repository) + "Processor"))
                .addStatement("return processor")
                .build();

        TypeSpec handler = TypeSpec.classBuilder(handlerClassName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(superclass)
                .addAnnotation(Component.class)
                .addField(processorField)
                .addMethod(getProcessorMethod)
                .build();

        writeJavaFile(packageName + ".handler", handler);
    }

    /**
     * Generates a KeyExpirationListener class for the given entity.
     * This is only generated if the entity uses the REFRESH_AHEAD caching pattern.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     */
    private void generateKeyExpirationListener(String packageName, String className,
                                               TypeElement entityElement) {
        String listenerClassName = className + "KeyExpirationListener";

        // Create the superclass type with generic parameters
        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".listener", "AbstractKeyExpirationListener"),
                TypeName.get(entityElement.asType())
        );

        // Create the service field
        FieldSpec serviceField = FieldSpec.builder(
                        ParameterizedTypeName.get(
                                ClassName.get(basePackage + ".service", "KinexisService"),
                                TypeName.get(entityElement.asType())
                        ),
                        "service",
                        Modifier.PROTECTED)
                .addAnnotation(Autowired.class)
                .build();

        // Create the getService method
        MethodSpec getServiceMethod = MethodSpec.methodBuilder("getService")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(serviceField.type())
                .addStatement("return service")
                .build();

        // Create the getKeyPrefix method
        MethodSpec getKeyPrefixMethod = MethodSpec.methodBuilder("getKeyPrefix")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(String.class)
                .addStatement("return \"$L:\"", entityElement.getQualifiedName().toString())
                .build();

        // Create the class
        TypeSpec keyExpirationListener = TypeSpec.classBuilder(listenerClassName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(superclass)
                .addAnnotation(Component.class)
                .addField(serviceField)
                .addMethod(getServiceMethod)
                .addMethod(getKeyPrefixMethod)
                .build();

        // Write the file
        writeJavaFile(packageName + ".listener", keyExpirationListener);
    }

    /**
     * Writes a generated Java file to the file system.
     *
     * @param packageName the package name for the generated class
     * @param typeSpec the type specification to write
     * @throws IOException if an I/O error occurs
     */
    private void writeJavaFile(String packageName, TypeSpec typeSpec) {
        try {
            JavaFile.builder(packageName, typeSpec)
                    .build()
                    .writeTo(filer);
        } catch (IOException e) {
            error(null, "Could not write file: %s", e.getMessage());
        }
    }

    /**
     * Reports an error during annotation processing.
     *
     * @param element the element where the error occurred
     * @param message the error message format
     * @param args the message arguments
     */
    private void error(Element element, String message, Object... args) {
        messager.printMessage(Diagnostic.Kind.ERROR,
                String.format(message, args), element);
    }

    private void warning(String message, Object... args) {
        messager.printMessage(Diagnostic.Kind.WARNING, String.format(message, args));
    }

    private void warning(Element element, String message, Object... args) {
        messager.printMessage(Diagnostic.Kind.WARNING, String.format(message, args), element);
    }

    private String getRepositoryInstanceName(TypeName repository) {
        final String className = repository.toString().substring(repository.toString().lastIndexOf(".")+1);
        return className.substring(0, 1).toLowerCase() + className.substring(1);
    }

    private String getRepositorySimpleName(TypeName repository) {
        return repository.toString().substring(repository.toString().lastIndexOf(".")+1);
    }

}