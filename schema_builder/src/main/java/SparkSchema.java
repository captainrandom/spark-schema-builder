import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Created by markjackson on 3/18/17.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface SparkSchema {
    boolean isNullable() default true;
    int order() default 0;
}
