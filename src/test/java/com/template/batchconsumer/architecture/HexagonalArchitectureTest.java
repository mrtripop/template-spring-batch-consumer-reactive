package com.template.batchconsumer.architecture;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.Architectures.layeredArchitecture;

class HexagonalArchitectureTest {

    private static final String BASE_PACKAGE = "com.template.batchconsumer";

    private final JavaClasses classes = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages(BASE_PACKAGE);

    @Test
    void hexagonalLayersRespectDependencyDirection() {
        ArchRule rule = layeredArchitecture()
                .consideringOnlyDependenciesInLayers()
                .layer("Domain").definedBy(BASE_PACKAGE + ".domain..")
                .layer("Application").definedBy(BASE_PACKAGE + ".application..")
                .layer("AdapterIn").definedBy(BASE_PACKAGE + ".adapter.in..")
                .layer("AdapterOut").definedBy(BASE_PACKAGE + ".adapter.out..")
                .layer("Config").definedBy(BASE_PACKAGE + ".config..")

                .whereLayer("Domain").mayOnlyBeAccessedByLayers("Application", "AdapterIn", "AdapterOut", "Config")
                .whereLayer("Application").mayOnlyBeAccessedByLayers("AdapterIn", "AdapterOut", "Config")
                .whereLayer("AdapterIn").mayOnlyBeAccessedByLayers("Config")
                .whereLayer("AdapterOut").mayOnlyBeAccessedByLayers("Config")
                .whereLayer("Config").mayNotBeAccessedByAnyLayer()
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    void applicationDependsOnlyOnDomainAndObservabilityFacades() {
        ArchRule rule = noClasses()
                .that().resideInAPackage(BASE_PACKAGE + ".application..")
                .should().dependOnClassesThat()
                .resideInAnyPackage("org.springframework..", "org.apache.kafka..", "io.r2dbc..", "reactor.kafka..")
                .because("application must stay framework-agnostic except for cross-cutting "
                        + "observability (Micrometer/SLF4J) — Spring, the Kafka client, R2DBC, "
                        + "and Reactor Kafka are explicitly kept out")
                .allowEmptyShould(true);

        rule.check(classes);
    }

    @Test
    void adapterInDoesNotDependOnAdapterOutDirectly() {
        ArchRule rule = noClasses()
                .that().resideInAPackage(BASE_PACKAGE + ".adapter.in..")
                .should().dependOnClassesThat()
                .resideInAPackage(BASE_PACKAGE + ".adapter.out..")
                .because("adapter.in must reach adapter.out functionality only through "
                        + "application.port.out interfaces")
                .allowEmptyShould(true);

        rule.check(classes);
    }
}
