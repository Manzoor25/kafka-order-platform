package io.orderplatform.events;

import org.apache.avro.util.ClassSecurityValidator;

/**
 * Registers the generated SpecificRecord package with Apache Avro 1.12.2's ClassSecurityValidator
 * allowlist (introduced in CVE hardening for Avro 1.12.2).
 */
public final class AvroSecurityConfig {

    public static final String TRUSTED_AVRO_PACKAGE = "io.orderplatform.events.avro";

    private static volatile boolean registered = false;

    private AvroSecurityConfig() {
    }

    public static void registerTrustedPackages() {
        if (!registered) {
            synchronized (AvroSecurityConfig.class) {
                if (!registered) {
                    ClassSecurityValidator.setGlobal(ClassSecurityValidator.composite(
                            ClassSecurityValidator.DEFAULT,
                            ClassSecurityValidator.builder().add(TRUSTED_AVRO_PACKAGE).build()
                    ));
                    registered = true;
                }
            }
        }
    }
}
