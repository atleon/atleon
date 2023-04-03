package io.atleon.spring;

import io.atleon.core.AloStream;
import org.springframework.stereotype.Component;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to be applied to {@link io.atleon.core.AloStreamConfig AloStreamConfig} types to have
 * Spring automatically bind the Config to an {@link io.atleon.core.AloStream AloStream}
 * implementation. When the type is not explicitly specified, a compatible stream registered with
 * Spring is searched for. When the type is explicitly specified, an instance of that type
 * registered with Spring is searched for; If it is not found, then an instance is created.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Component
public @interface AutoConfigureStream {

    Class<? extends AloStream> value() default AloStream.class;
}
