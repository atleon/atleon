package io.atleon.aws.testcontainers;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;

/**
 * Extension of {@link LocalStackContainer} that is a bit more convenient to use when testing
 * Atleon client code. Initializes with services supported by Atleon and contained in a module that
 * redefines certain AWS SDK V1 classes that the parent Container requires for class loading, but
 * does not actually need/use. Also provides convenience methods that help avoid direct dependency
 * on Testcontainers.
 */
public class AtleonLocalStackContainer extends LocalStackContainer {

    public AtleonLocalStackContainer() {
        super(DockerImageName.parse("localstack/localstack").withTag("0.14.5"));
        withServices(Service.SNS, Service.SQS);
    }

    public URI getSnsEndpointOverride() {
        return getEndpointOverride(Service.SNS);
    }

    public URI getSqsEndpointOverride() {
        return getEndpointOverride(Service.SQS);
    }
}
