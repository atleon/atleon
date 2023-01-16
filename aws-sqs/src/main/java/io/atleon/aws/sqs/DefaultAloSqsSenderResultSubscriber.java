package io.atleon.aws.sqs;

import io.atleon.core.Alo;
import reactor.core.publisher.BaseSubscriber;

public class DefaultAloSqsSenderResultSubscriber<C> extends BaseSubscriber<Alo<SqsSenderResult<C>>> {

    @Override
    protected void hookOnNext(Alo<SqsSenderResult<C>> value) {
        SqsSenderResult<C> result = value.get();
        if (result.isFailure()) {
            Alo.nacknowledge(value, result.error());
        } else {
            Alo.acknowledge(value);
        }
    }
}
