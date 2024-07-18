package io.atleon.core;

import io.atleon.util.Throwing;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.InvocationHandlerAdapter;
import net.bytebuddy.matcher.ElementMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * An {@link AloStream} that is a composite of others, providing utilities to multiply and/or
 * combine other {@link AloStream}s as a single resource.
 *
 * @param <C> The type of {@link AloStreamConfig} used to configure component streams
 */
public class CompositeAloStream<C extends AloStreamConfig> extends AloStream<C> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositeAloStream.class);

    private static final Map<Class<?>, ConfigCopier<?>> CONFIG_COPIERS_BY_TYPE = new ConcurrentHashMap<>();

    private final List<AloStream<? super C>> componentStreams;

    private CompositeAloStream(List<AloStream<? super C>> componentStreams) {
        this.componentStreams = componentStreams;
    }

    public static <C extends AloStreamConfig> AloStream<? super C>
    nCopies(int count, Supplier<AloStream<? super C>> creator) {
        return nCopies(count, creator.get(), creator);
    }

    public static <C extends AloStreamConfig> AloStream<? super C>
    nCopies(int count, AloStream<? super C> initial, Supplier<AloStream<? super C>> creator) {
        if (count < 0) {
            throw new IllegalArgumentException("Copy count must be non-negative where initial=" + initial);
        } else if (count == 0) {
            return new CompositeAloStream<>(Collections.emptyList());
        } else if (count == 1) {
            return initial;
        } else {
            Set<AloStream<? super C>> uniqueStreams = Collections.newSetFromMap(new IdentityHashMap<>());
            uniqueStreams.add(initial);
            for (int i = 1; i < count; i++) {
                if (!uniqueStreams.add(creator.get())) {
                    throw new IllegalStateException("Copies must be identity-unique where initial=" + initial);
                }
            }
            return new CompositeAloStream<C>(new ArrayList<>(uniqueStreams));
        }
    }

    public int componentStreamCount() {
        return componentStreams.size();
    }

    public AloStream<? super C> componentStreamAt(int index) {
        return componentStreams.get(index);
    }

    @Override
    protected Disposable startDisposable(C config) {
        ConfigCopier<C> configCopier = getOrCreateConfigCopier(config.getClass());
        Disposable.Composite disposables = Disposables.composite();
        for (int i = 0; i < componentStreams.size(); i++) {
            componentStreams.get(i).start(configCopier.copy(config, i + 1));
            disposables.add(new ComponentStreamDisposable(componentStreams.get(i)));
        }
        return disposables.size() == 0 ? Disposables.disposed() : disposables;
    }

    private static <C> ConfigCopier<C> getOrCreateConfigCopier(Class<?> configType) {
        return (ConfigCopier<C>) CONFIG_COPIERS_BY_TYPE.computeIfAbsent(configType, ByteBuddyConfigCopier::new);
    }

    private interface ConfigCopier<C> {

        C copy(C config, int id);
    }

    private static final class ByteBuddyConfigCopier<C> implements ConfigCopier<C>, InvocationHandler {

        private final Class<? extends C> proxiedConfigType;

        public ByteBuddyConfigCopier(Class<? extends C> configType) {
            this.proxiedConfigType = new ByteBuddy()
                .subclass(configType, newProxyConstructorStrategy(configType))
                .defineField("proxyId", Integer.class, Visibility.PUBLIC)
                .defineField("delegate", configType, Visibility.PUBLIC)
                .method(ElementMatchers.any())
                .intercept(InvocationHandlerAdapter.of(this))
                .make()
                .load(configType.getClassLoader())
                .getLoaded();
        }

        @Override
        public C copy(C config, int id) {
            try {
                C proxiedConfig = proxiedConfigType.getDeclaredConstructor().newInstance();
                proxiedConfigType.getDeclaredField("proxyId").set(proxiedConfig, id);
                proxiedConfigType.getDeclaredField("delegate").set(proxiedConfig, config);
                return proxiedConfig;
            } catch (ReflectiveOperationException e) {
                throw Throwing.propagate(e);
            }
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Class<?> proxyType = proxy.getClass();
            Object delegateValue = method.invoke(proxyType.getDeclaredField("delegate").get(proxy), args);
            return isNameMethod(method)
                ? delegateValue + "-" + proxyType.getDeclaredField("proxyId").get(proxy)
                : delegateValue;
        }

        private static ConstructorStrategy newProxyConstructorStrategy(Class<?> type) {
            Constructor<?> constructor = Arrays.stream(type.getDeclaredConstructors())
                .reduce((ctor1, ctor2) -> ctor1.getParameterCount() > ctor2.getParameterCount() ? ctor1 : ctor2)
                .orElseThrow(() -> new IllegalStateException("No constructor found for type=" + type));
            return new ConstructorStrategy.ForDefaultConstructor(
                ElementMatchers.takesArguments(constructor.getParameterTypes()));
        }

        private static boolean isNameMethod(Method method) {
            return method.getName().equals("name")
                && method.getParameterCount() == 0
                && method.getReturnType() == String.class;
        }
    }

    private static final class ComponentStreamDisposable implements Disposable {

        private final AloStream<?> componentStream;

        private final AtomicBoolean disposed = new AtomicBoolean(false);

        public ComponentStreamDisposable(AloStream<?> componentStream) {
            this.componentStream = componentStream;
        }

        @Override
        public void dispose() {
            if (disposed.compareAndSet(false, true)) {
                try {
                    componentStream.stop();
                } catch (Exception e) {
                    LOGGER.error("Failed to stop componentStream={}", componentStream, e);
                }
            }
        }

        @Override
        public boolean isDisposed() {
            return disposed.get();
        }
    }
}
