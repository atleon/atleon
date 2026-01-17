package io.atleon.aws.util;

import io.atleon.util.ConfigLoading;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import java.util.Map;
import java.util.Optional;

/**
 * Utility class used to build AWS resources from property-based configs
 */
public final class AwsConfig {

    public static final String CONFIG_PREFIX = "aws.";

    /**
     * The type of {@link AwsCredentialsProvider} to use. The "default" Provider uses a "chain" of
     * Providers which are capable of sourcing {@link AwsCredentials} from various environmental
     * locations. The "web-identity-token-file" Provider loads temporary credentials from
     * Java system properties or environment variables. When set to "static", the Credentials must be
     * explicitly provided by setting the Access Key ID and Secret Access Key.
     */
    public static final String CREDENTIALS_PROVIDER_TYPE_CONFIG = CONFIG_PREFIX + "credentials.provider.type";

    public static final String CREDENTIALS_PROVIDER_TYPE_DEFAULT = "default";

    public static final String CREDENTIALS_PROVIDER_TYPE_STATIC = "static";

    public static final String CREDENTIALS_PROVIDER_TYPE_WEB_IDENTITY_TOKEN_FILE = "web-identity-token-file";

    /**
     * When {@link AwsCredentials} are explicitly provided, this configures their type. The default
     * of "basic" simply requires the Access Key ID and Secret Access Key. The "session" type
     * requires an additional Session Token.
     */
    public static final String CREDENTIALS_TYPE_CONFIG = CONFIG_PREFIX + "credentials.type";

    public static final String CREDENTIALS_TYPE_BASIC = "basic";

    public static final String CREDENTIALS_TYPE_SESSION = "session";

    /**
     * When providing {@link AwsCredentials} explicitly, the value of this property should be the
     * Access Key ID.
     */
    public static final String CREDENTIALS_ACCESS_KEY_ID_CONFIG = CONFIG_PREFIX + "credentials.access.key.id";

    /**
     * When providing {@link AwsCredentials} explicitly, the value of this property should be the
     * Secret Access Key.
     */
    public static final String CREDENTIALS_SECRET_ACCESS_KEY_CONFIG = CONFIG_PREFIX + "credentials.secret.access.key";

    /**
     * When providing "session" {@link AwsCredentials} explicitly, the value of this property
     * should be the Secret Token.
     */
    public static final String CREDENTIALS_SESSION_TOKEN_CONFIG = CONFIG_PREFIX + "credentials.session.token";

    /**
     * If/when the AWS Region cannot be inferred by the underlying AWS Client, this property can be
     * set to explicitly provide it.
     */
    public static final String REGION_CONFIG = CONFIG_PREFIX + "region";

    private AwsConfig() {}

    public static AwsCredentialsProvider loadCredentialsProvider(Map<String, ?> configs) {
        String type = ConfigLoading.loadString(configs, CREDENTIALS_PROVIDER_TYPE_CONFIG)
                .orElse(CREDENTIALS_PROVIDER_TYPE_DEFAULT);
        switch (type) {
            case CREDENTIALS_PROVIDER_TYPE_DEFAULT:
                return DefaultCredentialsProvider.create();
            case CREDENTIALS_PROVIDER_TYPE_STATIC:
                return StaticCredentialsProvider.create(loadAwsCredentials(configs));
            case CREDENTIALS_PROVIDER_TYPE_WEB_IDENTITY_TOKEN_FILE:
                return WebIdentityTokenFileCredentialsProvider.create();
            default:
                throw new IllegalArgumentException("Cannot create AwsCredentialsProvider for type=" + type);
        }
    }

    public static AwsCredentials loadAwsCredentials(Map<String, ?> configs) {
        String type = ConfigLoading.loadString(configs, CREDENTIALS_TYPE_CONFIG).orElse(CREDENTIALS_TYPE_BASIC);
        switch (type) {
            case CREDENTIALS_TYPE_BASIC:
                return AwsBasicCredentials.create(
                        ConfigLoading.loadStringOrThrow(configs, CREDENTIALS_ACCESS_KEY_ID_CONFIG),
                        ConfigLoading.loadStringOrThrow(configs, CREDENTIALS_SECRET_ACCESS_KEY_CONFIG));
            case CREDENTIALS_TYPE_SESSION:
                return AwsSessionCredentials.create(
                        ConfigLoading.loadStringOrThrow(configs, CREDENTIALS_ACCESS_KEY_ID_CONFIG),
                        ConfigLoading.loadStringOrThrow(configs, CREDENTIALS_SECRET_ACCESS_KEY_CONFIG),
                        ConfigLoading.loadStringOrThrow(configs, CREDENTIALS_SESSION_TOKEN_CONFIG));
            default:
                throw new IllegalArgumentException("Cannot create AwsCredentials for type=" + type);
        }
    }

    public static Optional<Region> loadRegion(Map<String, ?> configs) {
        return ConfigLoading.loadParseable(configs, REGION_CONFIG, Region.class, Region::of);
    }
}
