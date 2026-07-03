package com.foogaro.kinexis.core.processor;

import com.foogaro.kinexis.core.annotation.CachingPatterns;
import com.foogaro.kinexis.core.model.CachingFormat;
import com.foogaro.kinexis.core.model.CachingPattern;
import com.palantir.javapoet.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import javax.annotation.processing.*;
import javax.annotation.processing.AbstractProcessor;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.*;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

/**
 * Annotation processor for the {@link CachingPatterns} annotation.
 * This processor generates the necessary classes for implementing caching patterns
 * in Redis, including StreamListeners, Processors,
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
     * 1. Generates a Redis repository for the entity cache
     * 2. Generates one StreamListener, Processor, and PendingMessageHandler for each entity
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
            return !annotations.isEmpty();
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
                CachingPatterns cachingPatterns = element.getAnnotation(CachingPatterns.class);
                String className = entityElement.getSimpleName().toString();
                String packageName = elementUtils.getPackageOf(entityElement).getQualifiedName().toString() + "." + className.toLowerCase();
                TypeName entityType = TypeName.get(entityElement.asType());
                generateEntityRegistry(packageName, className, entityElement);
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE, "\tEntityRegistry for: " + entityElement);
                if (!cachingPatterns.enabled()) {
                    processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,
                            "Skipping Kinexis component generation for disabled entity: " + entityElement);
                    continue;
                }
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"Generating RedisRepository and entity stream components");
                String newRedisRepository = generateRedisRepository(packageName, className, entityElement);
                if (newRedisRepository == null) {
                    continue;
                }
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tRedisRepository for: " + entityElement);
                generateStreamListener(packageName, className, entityElement, entityType);
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tStreamListener for: " + entityElement);
                generateProcessor(packageName, className, entityElement, entityType);
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tProcessor for: " + entityElement);
                generatePendingMessageHandler(packageName, className, entityElement, entityType);
                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"\tPendingMessageHandler for: " + entityElement);

                processingEnv.getMessager().printMessage(Diagnostic.Kind.NOTE,"Generating KeyExpirationListener(s)");
                Arrays.stream(cachingPatterns.patterns())
                        .filter(pattern -> pattern.getValue() == CachingPattern.REFRESH_AHEAD.getValue())
                        .filter(pattern -> cachingPatterns.ttl() > 0)
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
     * Generates a StreamListener class for the given entity.
     * The generated class extends {@link com.foogaro.kinexis.core.listener.AbstractStreamListener} and includes
     * necessary fields and methods for handling Redis Stream messages.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param streamType the stream target type
     */
    private void generateStreamListener(String packageName, String className,
                                        TypeElement entityElement, TypeName streamType) {
        String listenerClassName = getTypeSimpleName(streamType) + "StreamListener";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".listener", "AbstractStreamListener"),
                TypeName.get(entityElement.asType())
        );

        FieldSpec processorField = FieldSpec.builder(
                        ClassName.get(packageName + ".processor", getTypeSimpleName(streamType) + "Processor"),
                        getTypeInstanceName(streamType) + "Processor",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        MethodSpec getProcessorMethod = MethodSpec.methodBuilder("getProcessor")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ClassName.get(packageName + ".processor", getTypeSimpleName(streamType) + "Processor"))
                .addStatement("return " + getTypeInstanceName(streamType) + "Processor")
                .build();

        TypeSpec streamListenerClass = TypeSpec.classBuilder(listenerClassName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(superclass)
                .addAnnotation(Component.class)
                .addField(processorField)
                .addMethod(getProcessorMethod)
                .build();

        writeJavaFile(packageName + ".listener", streamListenerClass);
    }

    /**
     * Generates a Processor class for the given entity.
     * The generated class implements the necessary processing logic for Redis Stream messages.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param streamType the stream target type
     */
    private void generateProcessor(String packageName, String className,
                                   TypeElement entityElement, TypeName streamType) {
        String processorClassName = getTypeSimpleName(streamType) + "Processor";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".processor", "AbstractProcessor"),
                TypeName.get(entityElement.asType())
        );

        TypeSpec processorClass = TypeSpec.classBuilder(processorClassName)
                .addModifiers(Modifier.PUBLIC)
                .superclass(superclass)
                .addAnnotation(Component.class)
                .build();

        writeJavaFile(packageName + ".processor", processorClass);
    }

    /**
     * Generates a PendingMessageHandler class for the given entity.
     * The generated class handles messages that have been delivered but not acknowledged.
     *
     * @param packageName the package name for the generated class
     * @param className the entity class name
     * @param entityElement the entity type element
     * @param streamType the stream target type
     */
    private void generatePendingMessageHandler(String packageName, String className,
                                               TypeElement entityElement, TypeName streamType) {
        String handlerClassName = getTypeSimpleName(streamType) + "PendingMessageHandler";

        TypeName superclass = ParameterizedTypeName.get(
                ClassName.get(basePackage + ".handler", "AbstractPendingMessageHandler"),
                TypeName.get(entityElement.asType())
        );

        FieldSpec processorField = FieldSpec.builder(
                        ClassName.get(packageName + ".processor", getTypeSimpleName(streamType) + "Processor"),
                        "processor",
                        Modifier.PRIVATE)
                .addAnnotation(Autowired.class)
                .build();

        MethodSpec getProcessorMethod = MethodSpec.methodBuilder("getProcessor")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(ClassName.get(packageName + ".processor", getTypeSimpleName(streamType) + "Processor"))
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

    private void generateEntityRegistry(String packageName, String className, TypeElement entityElement) {
        TypeName classWildcard = ParameterizedTypeName.get(
                ClassName.get(Class.class),
                WildcardTypeName.subtypeOf(Object.class));
        TypeName entityTypesReturnType = ParameterizedTypeName.get(ClassName.get(Set.class), classWildcard);

        MethodSpec entityTypesMethod = MethodSpec.methodBuilder("entityTypes")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PUBLIC)
                .returns(entityTypesReturnType)
                .addStatement("return $T.of($T.class)", ClassName.get(Set.class), TypeName.get(entityElement.asType()))
                .build();

        TypeSpec registry = TypeSpec.classBuilder(className + "KinexisEntityRegistry")
                .addModifiers(Modifier.PUBLIC)
                .addSuperinterface(ClassName.get(basePackage + ".service", "KinexisEntityRegistry"))
                .addAnnotation(Component.class)
                .addMethod(entityTypesMethod)
                .build();

        writeJavaFile(packageName, registry);
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
                .addStatement("return $T.getEntityKeyPrefix($T.class) + \":\"",
                        ClassName.get(basePackage, "Misc"),
                        TypeName.get(entityElement.asType()))
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

    private String getTypeInstanceName(TypeName typeName) {
        final String className = typeName.toString().substring(typeName.toString().lastIndexOf(".")+1);
        return className.substring(0, 1).toLowerCase() + className.substring(1);
    }

    private String getTypeSimpleName(TypeName typeName) {
        return typeName.toString().substring(typeName.toString().lastIndexOf(".")+1);
    }

}
