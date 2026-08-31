package com.template.batchconsumer.architecture;

import com.template.batchconsumer.Application;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

@DisplayName("Hexagonal architecture boundaries")
class HexagonalArchitectureTest {

    // Derived from Application's actual package (not a literal) so a package rename can't
    // silently desync this test from reality.
    private static final String BASE_PACKAGE = Application.class.getPackageName();

    // Each layer's package suffix is defined once here and reused across every rule below —
    // a package rename only needs updating in one place instead of every rule that mentions it.
    private static final String DOMAIN_PACKAGE = BASE_PACKAGE + ".domain";
    private static final String APPLICATION_PACKAGE = BASE_PACKAGE + ".application";
    private static final String ADAPTER_IN_PACKAGE = BASE_PACKAGE + ".adapter.in";
    private static final String ADAPTER_OUT_PACKAGE = BASE_PACKAGE + ".adapter.out";
    private static final String CONFIG_PACKAGE = BASE_PACKAGE + ".config";

    // Layer names, each defined once and reused everywhere the layered-architecture rule
    // both declares a layer and references it in an access rule.
    private static final String DOMAIN_LAYER = "Domain";
    private static final String APPLICATION_LAYER = "Application";
    private static final String ADAPTER_IN_LAYER = "AdapterIn";
    private static final String ADAPTER_OUT_LAYER = "AdapterOut";
    private static final String CONFIG_LAYER = "Config";

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages(BASE_PACKAGE);

    @Test
    @DisplayName("layers only depend in the allowed direction")
    void hexagonalLayersRespectDependencyDirection() {
        // Arrange & Act
        ArchRule rule = layeredArchitecture()
                .consideringOnlyDependenciesInLayers()
                .layer(DOMAIN_LAYER).definedBy(DOMAIN_PACKAGE + "..")
                .layer(APPLICATION_LAYER).definedBy(APPLICATION_PACKAGE + "..")
                .layer(ADAPTER_IN_LAYER).definedBy(ADAPTER_IN_PACKAGE + "..")
                .layer(ADAPTER_OUT_LAYER).definedBy(ADAPTER_OUT_PACKAGE + "..")
                .layer(CONFIG_LAYER).definedBy(CONFIG_PACKAGE + "..")

                .whereLayer(DOMAIN_LAYER).mayOnlyBeAccessedByLayers(APPLICATION_LAYER, ADAPTER_IN_LAYER, ADAPTER_OUT_LAYER, CONFIG_LAYER)
                .whereLayer(APPLICATION_LAYER).mayOnlyBeAccessedByLayers(ADAPTER_IN_LAYER, ADAPTER_OUT_LAYER, CONFIG_LAYER)
                .whereLayer(ADAPTER_IN_LAYER).mayOnlyBeAccessedByLayers(CONFIG_LAYER)
                .whereLayer(ADAPTER_OUT_LAYER).mayOnlyBeAccessedByLayers(CONFIG_LAYER)
                .whereLayer(CONFIG_LAYER).mayNotBeAccessedByAnyLayer()
                .allowEmptyShould(true);

        // Assert
        rule.check(classes);
    }

    @Test
    @DisplayName("application stays framework-agnostic except observability facades")
    void applicationDependsOnlyOnDomainAndObservabilityFacades() {
        // Arrange & Act
        ArchRule rule = noClasses()
                .that().resideInAPackage(APPLICATION_PACKAGE + "..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..", "org.apache.kafka..", "io.r2dbc..", "reactor.kafka..")
                .because("application must stay framework-agnostic except for cross-cutting "
                        + "observability (Micrometer/SLF4J) — Spring, the Kafka client, R2DBC, "
                        + "and Reactor Kafka are explicitly kept out")
                .allowEmptyShould(true);

        // Assert
        rule.check(classes);
    }

    @Test
    @DisplayName("adapter.in never bypasses ports to reach adapter.out directly")
    void adapterInDoesNotDependOnAdapterOutDirectly() {
        // Arrange & Act
        ArchRule rule = noClasses()
                .that().resideInAPackage(ADAPTER_IN_PACKAGE + "..")
                .should().dependOnClassesThat()
                .resideInAPackage(ADAPTER_OUT_PACKAGE + "..")
                .because("adapter.in must reach adapter.out functionality only through "
                        + "application.port.out interfaces")
                .allowEmptyShould(true);

        // Assert
        rule.check(classes);
    }
}
