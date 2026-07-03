package com.foogaro.kinexis.core;

import com.foogaro.kinexis.core.handler.AbstractPendingMessageHandler;
import com.foogaro.kinexis.core.listener.AbstractKeyExpirationListener;
import com.foogaro.kinexis.core.listener.AbstractStreamListener;
import com.foogaro.kinexis.core.processor.AbstractProcessor;
import com.foogaro.kinexis.core.processor.CachingPatternsAnnotationProcessor;
import com.foogaro.kinexis.core.service.KinexisEntityRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachingPatternsAnnotationProcessorCompilationTest {

    private static final String PACKAGE_NAME = "com.example.kinexis.generated";

    @TempDir
    Path tempDir;

    @Test
    void cacheAsideOnlyGeneratesCompilableCoreComponents() throws Exception {
        CompilationResult result = compile(entity("CacheAsideEntity",
                "patterns = {CachingPattern.CACHE_ASIDE}"));

        assertCompiled(result);
        assertGenerated(result, "cacheasideentity/CacheAsideEntityKinexisEntityRegistry.java");
        assertGenerated(result, "cacheasideentity/repository/CacheAsideEntityRedisRepository.java");
        assertGenerated(result, "cacheasideentity/listener/CacheAsideEntityStreamListener.java");
        assertGenerated(result, "cacheasideentity/processor/CacheAsideEntityProcessor.java");
        assertGenerated(result, "cacheasideentity/handler/CacheAsideEntityPendingMessageHandler.java");
        assertNotGenerated(result, "cacheasideentity/listener/CacheAsideEntityKeyExpirationListener.java");

        try (URLClassLoader classLoader = classLoader(result)) {
            Class<?> entityClass = classLoader.loadClass(PACKAGE_NAME + ".CacheAsideEntity");
            Class<?> registryClass = classLoader.loadClass(PACKAGE_NAME + ".cacheasideentity.CacheAsideEntityKinexisEntityRegistry");
            Class<?> processorClass = classLoader.loadClass(PACKAGE_NAME + ".cacheasideentity.processor.CacheAsideEntityProcessor");
            Class<?> listenerClass = classLoader.loadClass(PACKAGE_NAME + ".cacheasideentity.listener.CacheAsideEntityStreamListener");
            Class<?> handlerClass = classLoader.loadClass(PACKAGE_NAME + ".cacheasideentity.handler.CacheAsideEntityPendingMessageHandler");

            assertTrue(KinexisEntityRegistry.class.isAssignableFrom(registryClass));
            assertTrue(AbstractProcessor.class.isAssignableFrom(processorClass));
            assertTrue(AbstractStreamListener.class.isAssignableFrom(listenerClass));
            assertTrue(AbstractPendingMessageHandler.class.isAssignableFrom(handlerClass));

            KinexisEntityRegistry registry = (KinexisEntityRegistry) registryClass.getDeclaredConstructor().newInstance();
            assertEquals(Set.of(entityClass), registry.entityTypes());

            AbstractProcessor<?> processor = (AbstractProcessor<?>) processorClass.getDeclaredConstructor().newInstance();
            assertEquals(entityClass, processor.getEntityClass());
        }
    }

    @Test
    void writeBehindOnlyGeneratesCompilableStreamComponents() throws Exception {
        CompilationResult result = compile(entity("WriteBehindEntity",
                "patterns = {CachingPattern.WRITE_BEHIND}"));

        assertCompiled(result);
        assertGenerated(result, "writebehindentity/WriteBehindEntityKinexisEntityRegistry.java");
        assertGenerated(result, "writebehindentity/repository/WriteBehindEntityRedisRepository.java");
        assertGenerated(result, "writebehindentity/listener/WriteBehindEntityStreamListener.java");
        assertGenerated(result, "writebehindentity/processor/WriteBehindEntityProcessor.java");
        assertGenerated(result, "writebehindentity/handler/WriteBehindEntityPendingMessageHandler.java");
        assertNotGenerated(result, "writebehindentity/listener/WriteBehindEntityKeyExpirationListener.java");
    }

    @Test
    void refreshAheadWithTtlGeneratesCompilableExpirationListener() throws Exception {
        CompilationResult result = compile(entity("RefreshAheadEntity",
                "patterns = {CachingPattern.REFRESH_AHEAD}, ttl = 30"));

        assertCompiled(result);
        assertGenerated(result, "refreshaheadentity/listener/RefreshAheadEntityKeyExpirationListener.java");

        try (URLClassLoader classLoader = classLoader(result)) {
            Class<?> listenerClass = classLoader.loadClass(PACKAGE_NAME + ".refreshaheadentity.listener.RefreshAheadEntityKeyExpirationListener");
            assertTrue(AbstractKeyExpirationListener.class.isAssignableFrom(listenerClass));
        }
    }

    @Test
    void disabledAnnotationOnlyGeneratesEntityRegistry() {
        CompilationResult result = compile(entity("DisabledEntity",
                "patterns = {CachingPattern.CACHE_ASIDE, CachingPattern.WRITE_BEHIND}, enabled = false, ttl = 30"));

        assertCompiled(result);
        assertGenerated(result, "disabledentity/DisabledEntityKinexisEntityRegistry.java");
        assertNotGenerated(result, "disabledentity/repository/DisabledEntityRedisRepository.java");
        assertNotGenerated(result, "disabledentity/listener/DisabledEntityStreamListener.java");
        assertNotGenerated(result, "disabledentity/processor/DisabledEntityProcessor.java");
        assertNotGenerated(result, "disabledentity/handler/DisabledEntityPendingMessageHandler.java");
        assertNotGenerated(result, "disabledentity/listener/DisabledEntityKeyExpirationListener.java");
    }

    @Test
    void customTargetSaveApiCompilesWithGeneratedWriteBehindEntity() {
        CompilationResult result = compile(
                entity("TargetedEntity", "patterns = {CachingPattern.WRITE_BEHIND}"),
                source(PACKAGE_NAME + ".TargetedEntityService", """
                        package %s;

                        import com.foogaro.kinexis.core.service.KinexisService;

                        public class TargetedEntityService extends KinexisService<TargetedEntity> {
                            public void saveToTargets(TargetedEntity entity) {
                                save(entity, "postgresql", "mysql", "primary");
                                delete(entity.getId(), "postgresql", "mysql", "primary");
                            }
                        }
                        """.formatted(PACKAGE_NAME)));

        assertCompiled(result);
        assertGenerated(result, "targetedentity/repository/TargetedEntityRedisRepository.java");
        assertGenerated(result, "targetedentity/processor/TargetedEntityProcessor.java");
    }

    @Test
    void missingIdFieldFailsCompilation() {
        CompilationResult result = compile(source(PACKAGE_NAME + ".MissingIdEntity", """
                package %s;

                import com.foogaro.kinexis.core.annotation.CachingPatterns;
                import com.foogaro.kinexis.core.model.CachingPattern;

                @CachingPatterns(patterns = {CachingPattern.CACHE_ASIDE})
                public class MissingIdEntity {
                    private String name;
                }
                """.formatted(PACKAGE_NAME)));

        assertFalse(result.success(), diagnostics(result));
        assertTrue(diagnostics(result).contains("must have a field annotated with @Id"),
                diagnostics(result));
    }

    private Source entity(String simpleName, String annotationAttributes) {
        return source(PACKAGE_NAME + "." + simpleName, """
                package %s;

                import com.foogaro.kinexis.core.annotation.CachingPatterns;
                import com.foogaro.kinexis.core.model.CachingPattern;
                import jakarta.persistence.Id;

                @CachingPatterns(%s)
                public class %s {
                    @Id
                    private Long id;
                    private String name;

                    public Long getId() {
                        return id;
                    }

                    public void setId(Long id) {
                        this.id = id;
                    }

                    public String getName() {
                        return name;
                    }

                    public void setName(String name) {
                        this.name = name;
                    }
                }
                """.formatted(PACKAGE_NAME, annotationAttributes, simpleName));
    }

    private Source source(String className, String content) {
        return new Source(className, content);
    }

    private CompilationResult compile(Source... sources) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler, "The annotation processor compilation tests require a JDK, not a JRE.");

        try {
            Path sourceDir = Files.createDirectories(tempDir.resolve("sources-" + System.nanoTime()));
            Path classesDir = Files.createDirectories(tempDir.resolve("classes-" + System.nanoTime()));
            Path generatedDir = Files.createDirectories(tempDir.resolve("generated-" + System.nanoTime()));

            List<Path> sourceFiles = Stream.of(sources)
                    .map(source -> writeSource(sourceDir, source))
                    .toList();
            DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
            try (StandardJavaFileManager fileManager = compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
                Iterable<? extends JavaFileObject> javaFiles = fileManager.getJavaFileObjectsFromPaths(sourceFiles);
                List<String> options = List.of(
                        "-classpath", compilerClasspath(),
                        "-processor", CachingPatternsAnnotationProcessor.class.getName(),
                        "-d", classesDir.toString(),
                        "-s", generatedDir.toString(),
                        "-source", "21",
                        "-target", "21"
                );
                Boolean success = compiler.getTask(null, fileManager, diagnostics, options, null, javaFiles).call();
                return new CompilationResult(Boolean.TRUE.equals(success), classesDir, generatedDir, diagnostics.getDiagnostics());
            }
        } catch (IOException e) {
            throw new AssertionError("Could not prepare annotation processor test compilation", e);
        }
    }

    private Path writeSource(Path sourceDir, Source source) {
        try {
            Path sourceFile = sourceDir.resolve(source.className().replace('.', '/') + ".java");
            Files.createDirectories(sourceFile.getParent());
            Files.writeString(sourceFile, source.content(), StandardCharsets.UTF_8);
            return sourceFile;
        } catch (IOException e) {
            throw new AssertionError("Could not write source for " + source.className(), e);
        }
    }

    private String compilerClasspath() {
        return Stream.of(
                        System.getProperty("surefire.test.class.path"),
                        System.getProperty("java.class.path"))
                .filter(value -> value != null && !value.isBlank())
                .flatMap(value -> Stream.of(value.split(File.pathSeparator)))
                .filter(value -> value != null && !value.isBlank())
                .distinct()
                .collect(Collectors.joining(File.pathSeparator));
    }

    private URLClassLoader classLoader(CompilationResult result) throws IOException {
        return new URLClassLoader(new URL[]{result.classesDir().toUri().toURL()},
                Thread.currentThread().getContextClassLoader());
    }

    private void assertCompiled(CompilationResult result) {
        assertTrue(result.success(), diagnostics(result));
    }

    private void assertGenerated(CompilationResult result, String relativePath) {
        Path sourceFile = generatedSource(result, relativePath);
        assertTrue(Files.exists(sourceFile), () -> "Expected generated source " + sourceFile + "\n" + diagnostics(result));
    }

    private void assertNotGenerated(CompilationResult result, String relativePath) {
        Path sourceFile = generatedSource(result, relativePath);
        assertFalse(Files.exists(sourceFile), () -> "Did not expect generated source " + sourceFile + "\n" + diagnostics(result));
    }

    private Path generatedSource(CompilationResult result, String relativePath) {
        return result.generatedDir()
                .resolve(PACKAGE_NAME.replace('.', '/'))
                .resolve(relativePath);
    }

    private String diagnostics(CompilationResult result) {
        return result.diagnostics().stream()
                .map(diagnostic -> diagnostic.getKind() + ": " + diagnostic.getMessage(null))
                .collect(Collectors.joining(System.lineSeparator()));
    }

    private record Source(String className, String content) {
    }

    private record CompilationResult(boolean success,
                                     Path classesDir,
                                     Path generatedDir,
                                     List<Diagnostic<? extends JavaFileObject>> diagnostics) {
    }
}
