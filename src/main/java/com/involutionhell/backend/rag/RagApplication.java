package com.involutionhell.backend.rag;


import com.involutionhell.backend.rag.infrastructure.nativeimage.RagNativeConfiguration;
import com.involutionhell.backend.rag.shared.properties.RagProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.modulith.Modulith;

@SpringBootApplication
@ConfigurationPropertiesScan(basePackageClasses = RagProperties.class)
@Modulith(
        systemName = "Involution Hell RAG Backend",
        sharedModules = "shared"
)
@ImportRuntimeHints(RagNativeConfiguration.RagRuntimeHintsRegistrar.class)
public class RagApplication {

    static void main(String[] args) {
        SpringApplication.run(RagApplication.class, args);
    }

}
