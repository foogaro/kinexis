package com.foogaro.kinexis.core.annotation;

import com.foogaro.kinexis.core.service.CachingPattern;
import com.palantir.javapoet.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import javax.annotation.processing.*;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
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

                if (repositories.isEmpty()) {
                    warning(entityElement, "No repositories found for entity %s", className);
                    continue;
                }

                // Create the classes for each repo found
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"Generating StreamListener(s), Processors(s), ProcessOrchestrator(s) and PendingMessageHandler(s)");
                for (TypeElement repository : repositories) {
                    String repositoryType = repository.getSimpleName().toString();
                    String repositoryPrefix = getRepositoryPrefix(repositoryType);

                    generateStreamListener(packageName, className, entityElement, repository, repositoryPrefix);
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tStreamListener for: " + repository);
                    generateProcessor(packageName, className, entityElement, repository, repositoryPrefix);
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tProcessor for: " + repository);
                    generateProcessOrchestrator(packageName, className, entityElement, repository, repositoryPrefix);
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tProcessOrchestrator for: " + repository);
                    generatePendingMessageHandler(packageName, className, entityElement, repository, repositoryPrefix);
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tPendingMessageHandler for: " + repository);
                }

                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"Generating KeyExpirationListener(s)");
                // Generate the key expiration listener for each element found, if has CachingPattern.REFRESH_AHEAD
                Arrays.stream(element.getAnnotation(CachingPatterns.class).patterns())
                        .filter(pattern -> pattern.getValue() == CachingPattern.REFRESH_AHEAD.getValue())
                        .findFirst()
                        .ifPresent(pattern -> {
                            generateKeyExpirationListener(packageName, className, entityElement);
                            processingEnv.getMessager().printMessage(
                                    Diagnostic.Kind.NOTE, "\tKeyExpirationListener for: " + entityElement
                                    + " | Package: " + packageName
                                    + " | Class: " + className
                            );
                        });
            }
        } catch (Exception e) {
            error(null, "Error processing @CachingPatterns annotation: %s", e.getMessage());
        }
        return true;
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

    /**
     * Gets the prefix for a repository type based on its name.
     * Extracts the technology-specific prefix (e.g., Jpa, Mongo, Redis, Cassandra)
     * or removes the "Repository" suffix.
     *
     * @param repositoryType the repository type name
     * @return the repository prefix
     */
    private String getRepositoryPrefix(String repositoryType) {
        if (repositoryType.startsWith("Jpa")) return "Jpa";
        if (repositoryType.startsWith("Mongo")) return "Mongo";
        if (repositoryType.startsWith("Redis")) return "Redis";
        if (repositoryType.startsWith("Cassandra")) return "Cassandra";
        return repositoryType.replaceAll("Repository$", "");
    }

    /**
     * Generates a StreamListener class for the given entity and repository.
     * The generated class extends {@link AbstractStreamListener} and includes
     * necessary fields and methods for handling Redis Stream messages.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param repository the repository type element
     * @param prefix the repository prefix
     */
    private void generateStreamListener(String packageName, String className,
                                        TypeElement entityElement, TypeElement repository, String prefix) {
        String listenerClassName = prefix + className + "StreamListener";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".listener", "AbstractStreamListener"),
                TypeName.get(entityElement.asType()),
                TypeName.get(repository.asType())
        );

        FieldSpec repositoryField = FieldSpec.builder(
                        TypeName.get(repository.asType()),
                        "employerRepository",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        FieldSpec redisTemplateField = FieldSpec.builder(
                        ParameterizedTypeName.get(
                                ClassName.get("org.springframework.data.redis.core", "RedisTemplate"),
                                ClassName.get(String.class),
                                ClassName.get(String.class)
                        ),
                        "redisTemplate",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        FieldSpec streamListenerContainerField = FieldSpec.builder(
                        ParameterizedTypeName.get(
                                ClassName.get("org.springframework.data.redis.stream", "StreamMessageListenerContainer"),
                                ClassName.get(String.class),
                                ParameterizedTypeName.get(
                                        ClassName.get("org.springframework.data.redis.connection.stream", "MapRecord"),
                                        ClassName.get(String.class),
                                        ClassName.get(String.class),
                                        ClassName.get(String.class)
                                )
                        ),
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
                        ClassName.get(packageName + ".processor", prefix + className + "ProcessOrchestrator"),
                        "processOrchestrator",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        FieldSpec processorField = FieldSpec.builder(
                        ClassName.get(packageName + ".processor", prefix + className + "Processor"),
                        "processor",
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
                .returns(streamListenerContainerField.type())
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
                .addStatement("employerRepository.deleteById(($T) id)", Long.class)
                .build();

        MethodSpec saveEntityMethod = MethodSpec.methodBuilder("saveEntity")
                .addModifiers(Modifier.PROTECTED)
                .addParameter(TypeName.get(entityElement.asType()), "entity")
                .returns(TypeName.get(entityElement.asType()))
                .addStatement("return employerRepository.save(entity)")
                .build();

        MethodSpec getProcessOrchestratorMethod = MethodSpec.methodBuilder("getProcessOrchestrator")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(processOrchestratorField.type())
                .addStatement("return processOrchestrator")
                .build();

        MethodSpec getProcessorMethod = MethodSpec.methodBuilder("getProcessor")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(processorField.type())
                .addStatement("return processor")
                .build();

        TypeSpec streamListener = TypeSpec.classBuilder(listenerClassName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(superclass)
                .addAnnotation(Component.class)
                .addField(repositoryField)
                .addField(redisTemplateField)
                .addField(streamListenerContainerField)
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

        writeJavaFile(packageName + ".listener", streamListener);
    }

    /**
     * Generates a Processor class for the given entity and repository.
     * The generated class implements the necessary processing logic for Redis Stream messages.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param repository the repository type element
     * @param prefix the repository prefix
     */
    private void generateProcessor(String packageName, String className,
                                   TypeElement entityElement, TypeElement repository, String prefix) {
        String processorClassName = prefix + className + "Processor";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".processor", "AbstractProcessor"),
                TypeName.get(entityElement.asType()),
                TypeName.get(repository.asType())
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

        TypeSpec processor = TypeSpec.classBuilder(processorClassName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(superclass)
                .addAnnotation(Component.class)
                .addField(beanFactoryField)
                .addField(beanFinderField)
                .addMethod(getRepositoryFinderMethod)
                .build();

        writeJavaFile(packageName + ".processor", processor);
    }

    /**
     * Generates a ProcessOrchestrator class for the given entity and repository.
     * The generated class coordinates the message processing flow.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param repository the repository type element
     * @param prefix the repository prefix
     */
    private void generateProcessOrchestrator(String packageName, String className,
                                             TypeElement entityElement, TypeElement repository, String prefix) {
        String orchestratorClassName = prefix + className + "ProcessOrchestrator";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".orchestrator", "AbstractProcessOrchestrator"),
                TypeName.get(entityElement.asType()),
                TypeName.get(repository.asType())
        );

        TypeSpec orchestrator = TypeSpec.classBuilder(orchestratorClassName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(superclass)
                .addAnnotation(Component.class)
                .build();

        writeJavaFile(packageName + ".processor", orchestrator);
    }

    /**
     * Generates a PendingMessageHandler class for the given entity and repository.
     * The generated class handles messages that have been delivered but not acknowledged.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param repository the repository type element
     * @param prefix the repository prefix
     */
    private void generatePendingMessageHandler(String packageName, String className,
                                               TypeElement entityElement, TypeElement repository, String prefix) {
        String handlerClassName = prefix + "PendingMessageHandler";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".handler", "AbstractPendingMessageHandler"),
                TypeName.get(entityElement.asType()),
                TypeName.get(repository.asType())
        );

        FieldSpec processorField = FieldSpec.builder(
                        ClassName.get(packageName + ".processor", prefix + className + "Processor"),
                        "processor",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        MethodSpec getProcessorMethod = MethodSpec.methodBuilder("getProcessor")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ClassName.get(packageName + ".processor", prefix + className + "Processor"))
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
                .addStatement("return \"$L:\"", className.toLowerCase())
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

    private void warning(Element element, String message, Object... args) {
        messager.printMessage(Diagnostic.Kind.WARNING, 
            String.format(message, args), element);
    }
}