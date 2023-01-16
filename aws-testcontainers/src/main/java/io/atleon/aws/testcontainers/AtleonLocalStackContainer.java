package io.atleon.aws.testcontainers;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;

public class AtleonLocalStackContainer extends LocalStackContainer {

    public AtleonLocalStackContainer() {
        super(DockerImageName.parse("localstack/localstack").withTag("0.14.5"));
        withServices(Service.SQS);
    }

    public URI getSqsEndpointOverride() {
        return getEndpointOverride(Service.SQS);
    }
}
