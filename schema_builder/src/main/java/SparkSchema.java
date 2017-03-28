import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An annotation to help determine non-default serialization/deserialization parameters in the SparkSchema for a given
 * object. Please see {@link SchemaBuilder}
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface SparkSchema {
    boolean isNullable() default true;
    int order() default 0;
}
