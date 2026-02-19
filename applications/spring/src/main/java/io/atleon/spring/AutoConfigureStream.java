package io.atleon.spring;

import io.atleon.core.AloStream;
import io.atleon.core.AloStreamConfig;
import org.springframework.stereotype.Component;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to be applied to {@link AloStreamConfig AloStreamConfig} types to have Spring
 * automatically bind the Config to an {@link AloStream AloStream} implementation. When the type is
 * not explicitly specified, a compatible stream registered with Spring is searched for. When the
 * type is explicitly specified, an instance of that type registered with Spring is searched for;
 * If it is not found, then an instance is created.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface AutoConfigureStream {

    /**
     * The type of {@link AloStream} to bind the annotated {@link io.atleon.core.AloStreamConfig} to
     */
    Class<? extends AloStream> value() default AloStream.class;

    /**
     * An expression that must resolve to an integral number, used to specify the number of stream
     * instances to instantiate, configure, and run. In order for more than one instance to be
     * created, it must be possible to create "unique" instances of the annotated
     * {@link AloStreamConfig}. For {@link io.atleon.core.SelfConfigurableAloStream} this is done
     * by setting the instance ID. For other configs, this is implemented using
     * <a href="https://bytebuddy.net">Byte Buddy</a>, which depends on being able to subclass
     * the annotated AloStreamConfig. Due to the semantics of extension, the annotated class must
     * be non-final and have some accessible constructor that accepts default values for all
     * parameters. When multiple constructors are available, preference is given to those with
     * fewer parameters. The extended classes override the {@link AloStreamConfig#name()} to ensure
     * configured {@link AloStream} instances are uniquely named (i.e. by appending an ID).
     */
    String instanceCountValue() default "1";
}
