package io.atleon.kafka.avro;

import io.atleon.schemaregistry.confluent.RegistrySerDeConfig;

@Deprecated
public abstract class AvroSerDe {

    public static final String SCHEMA_REGISTRY_URL_CONFIG = RegistrySerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

    public static final String BASIC_AUTH_CREDENTIALS_SOURCE = RegistrySerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;

    public static final String USER_INFO_CONFIG = RegistrySerDeConfig.USER_INFO_CONFIG;

    public static final String KEY_SUBJECT_NAME_STRATEGY = RegistrySerDeConfig.KEY_SUBJECT_NAME_STRATEGY;

    public static final String VALUE_SUBJECT_NAME_STRATEGY = RegistrySerDeConfig.VALUE_SUBJECT_NAME_STRATEGY;
}
