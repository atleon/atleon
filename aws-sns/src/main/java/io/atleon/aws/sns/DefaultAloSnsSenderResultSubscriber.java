package io.atleon.aws.sns;

import io.atleon.core.Alo;
import reactor.core.publisher.BaseSubscriber;

public class DefaultAloSnsSenderResultSubscriber<C> extends BaseSubscriber<Alo<SnsSenderResult<C>>> {

    @Override
    protected void hookOnNext(Alo<SnsSenderResult<C>> value) {
        SnsSenderResult<C> result = value.get();
        if (result.isFailure()) {
            Alo.nacknowledge(value, result.error());
        } else {
            Alo.acknowledge(value);
        }
    }
}
