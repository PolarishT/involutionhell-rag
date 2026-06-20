package com.involutionhell.backend.rag.infrastructure.nativeimage;

import com.involutionhell.backend.rag.retrieval.api.RagConversationSummaryView;
import org.springframework.aot.hint.MemberCategory;

import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;

/**
 * RAG GraalVM Native Image 适配配置。
 * <p>
 * 当前项目使用 JDBC，不再注册 MyBatis / MyBatis-Flex 相关 RuntimeHints。
 * <p>
 * 主要解决：
 * 1. Jackson 序列化 record / DTO / View / Response 时缺少反射信息；
 * 2. Java record 在 native image 中 getRecordComponents() 不可用；
 * 3. DTO 数组类型，例如 XxxView[] 缺少反射注册。
 */
@Configuration(proxyBeanMethods = false)
@ImportRuntimeHints(RagNativeConfiguration.RagRuntimeHintsRegistrar.class)
public class RagNativeConfiguration {

    /**
     * 业务根包。
     * <p>
     * 如果 DTO / View / Event / State 不只在 rag 包下，
     * 可以改成 "com.involutionhell.backend"。
     */
    private static final String BASE_PACKAGE = "com.involutionhell.backend.rag";

    /**
     * Jackson 处理 record / DTO / View 时需要的方法、字段、构造器信息。
     * <p>
     * 注意：
     * record component accessor 例如 xxx() 本质是 public method，
     * 所以必须保留 PUBLIC_METHODS 相关类别。
     */
    private static final MemberCategory[] JACKSON_MEMBERS = {
            MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
            MemberCategory.INVOKE_PUBLIC_CONSTRUCTORS,

            MemberCategory.INVOKE_DECLARED_METHODS,
            MemberCategory.INVOKE_PUBLIC_METHODS,

            MemberCategory.ACCESS_DECLARED_FIELDS,
            MemberCategory.ACCESS_PUBLIC_FIELDS
    };

    public static class RagRuntimeHintsRegistrar implements RuntimeHintsRegistrar {

        private static void registerCommonResources(RuntimeHints hints) {
            hints.resources().registerPattern("application.yml");
            hints.resources().registerPattern("application.yaml");
            hints.resources().registerPattern("application.properties");
            hints.resources().registerPattern("application-*.yml");
            hints.resources().registerPattern("application-*.yaml");
            hints.resources().registerPattern("application-*.properties");

            hints.resources().registerPattern("META-INF/services/*");
            hints.resources().registerPattern("META-INF/spring/*");
            hints.resources().registerPattern("META-INF/spring.factories");
            hints.resources().registerPattern(
                    "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports"
            );
        }

        private static void registerJacksonType(RuntimeHints hints, Class<?> type) {
            if (type == null) {
                return;
            }

            hints.reflection().registerType(type, JACKSON_MEMBERS);

            if (!type.isArray()) {
                Class<?> arrayType = java.lang.reflect.Array.newInstance(type, 0).getClass();
                hints.reflection().registerType(arrayType);
            }
        }

        @Override
        public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
            registerCommonResources(hints);

            // 你之前明确遇到过的 View，手动保留一份兜底。
            registerJacksonType(hints, RagConversationSummaryView.class);

            // 自动扫描 RAG 业务包下的 DTO / View / Response / Event / State / record 等。
            ApplicationNativeHintScanner scanner = new ApplicationNativeHintScanner();
            scanner.registerApplicationPackage(hints, classLoader, BASE_PACKAGE);
        }
    }

    /**
     * 扫描业务包，自动注册 Native Image 运行时需要的 DTO / record / View / Event 等。
     */
    static class ApplicationNativeHintScanner {

        private static final String CLASS_RESOURCE_PATTERN = "classpath*:%s/**/*.class";

        private final PathMatchingResourcePatternResolver resourceResolver =
                new PathMatchingResourcePatternResolver();

        private final CachingMetadataReaderFactory metadataReaderFactory =
                new CachingMetadataReaderFactory(resourceResolver);

        private static void registerJacksonType(RuntimeHints hints, Class<?> type) {
            hints.reflection().registerType(type, JACKSON_MEMBERS);

            if (!type.isArray()) {
                Class<?> arrayType = java.lang.reflect.Array.newInstance(type, 0).getClass();
                hints.reflection().registerType(arrayType);
            }
        }

        void registerApplicationPackage(RuntimeHints hints, ClassLoader classLoader, String basePackage) {
            Set<Class<?>> classes = scanClasses(classLoader, basePackage);

            int reflectionCount = 0;

            for (Class<?> clazz : classes) {
                if (shouldRegisterReflection(clazz)) {
                    registerJacksonType(hints, clazz);
                    reflectionCount++;
                }
            }

            System.out.println("[rag-native-hints] package=" + basePackage
                    + ", scanned=" + classes.size()
                    + ", reflection=" + reflectionCount);
        }

        private Set<Class<?>> scanClasses(ClassLoader classLoader, String basePackage) {
            String packageSearchPath = String.format(
                    CLASS_RESOURCE_PATTERN,
                    basePackage.replace('.', '/')
            );

            Set<Class<?>> result = new HashSet<>();

            try {
                Resource[] resources = resourceResolver.getResources(packageSearchPath);

                for (Resource resource : resources) {
                    if (!resource.isReadable()) {
                        continue;
                    }

                    MetadataReader metadataReader = metadataReaderFactory.getMetadataReader(resource);
                    String className = metadataReader.getClassMetadata().getClassName();

                    try {
                        Class<?> clazz = Class.forName(className, false, classLoader);
                        result.add(clazz);
                    } catch (ClassNotFoundException
                             | NoClassDefFoundError
                             | UnsupportedClassVersionError ignored) {
                        // optional 依赖缺失或版本不兼容时跳过，避免 AOT 阶段失败
                    }
                }
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to scan native hint package: " + basePackage, e
                );
            }

            return result;
        }

        private boolean shouldRegisterReflection(Class<?> clazz) {
            if (clazz.isInterface()) {
                return false;
            }

            if (clazz.isAnnotation()) {
                return false;
            }

            if (clazz.isAnonymousClass()) {
                return false;
            }

            if (clazz.isLocalClass()) {
                return false;
            }

            if (clazz.isSynthetic()) {
                return false;
            }

            if (clazz.isEnum()) {
                return true;
            }

            int modifiers = clazz.getModifiers();

            if (Modifier.isAbstract(modifiers)) {
                return false;
            }

            String name = clazz.getName();

            // Java record：Jackson 会调用 getRecordComponents()，native 下必须注册。
            if (clazz.isRecord()) {
                return true;
            }

            // 包名规则
            return name.contains(".entity.")
                    || name.contains(".entities.")
                    || name.contains(".model.")
                    || name.contains(".models.")
                    || name.contains(".dto.")
                    || name.contains(".api.")
                    || name.contains(".request.")
                    || name.contains(".response.")
                    || name.contains(".event.")
                    || name.contains(".events.")
                    || name.contains(".state.")
                    || name.contains(".states.")
                    || name.contains(".record.")
                    || name.contains(".records.")
                    || name.contains(".metadata.")
                    || name.contains(".support.")

                    // 类名规则
                    || name.endsWith("Entity")
                    || name.endsWith("DO")
                    || name.endsWith("DTO")
                    || name.endsWith("Dto")
                    || name.endsWith("VO")
                    || name.endsWith("View")
                    || name.endsWith("Request")
                    || name.endsWith("Response")
                    || name.endsWith("Result")
                    || name.endsWith("Event")
                    || name.endsWith("State")
                    || name.endsWith("Notice")
                    || name.endsWith("Chunk")
                    || name.endsWith("Filter")
                    || name.endsWith("Summary")
                    || name.endsWith("Message");
        }
    }
}