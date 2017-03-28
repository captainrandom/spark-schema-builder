import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Created by markjackson on 3/18/17.
 */
public class SchemaBuilder {

    public static <T> StructType determineSchema(Class<T> type) {
        // TODO: Probably want to filter out protected/private fields that don't have any getters.
        List<StructField> sparkFieldList = FieldUtils.getAllFieldsList(type).stream()
                .filter(field -> !Modifier.isStatic(field.getModifiers())) // don't get any static fields
                .sorted((field1, field2) -> {
                    SparkSchema schema1 = field1.getAnnotation(SparkSchema.class);
                    SparkSchema schema2 = field2.getAnnotation(SparkSchema.class);
                    int order1 = determineOrderNumer(field1, schema1);
                    int order2 = determineOrderNumer(field2, schema2);
                    return Integer.compare(order1,order2);
                })
                .map(field -> {
                    Optional<DataType> dataType = determineSimpleDataType(field.getType());
                    if(!dataType.isPresent()) {
                        // TODO: Need to find a better exception to throw there. Should be a non-handled exception type.
                        throw new RuntimeException("Cant find corresponding type for " + field.getType() + ". I have yet to implement " +
                                "reflection for more complex types (ex: nested types).");
                    }
                    SparkSchema schemaAnnotation = field.getAnnotation(SparkSchema.class);
                    boolean isNullable = schemaAnnotation == null || schemaAnnotation.isNullable();
                    return DataTypes.createStructField(field.getName(), dataType.get(), isNullable);
                }).collect(Collectors.toList());


        return new StructType(sparkFieldList.toArray(new StructField[sparkFieldList.size()]));
    }

    private static int determineOrderNumer(Field field, SparkSchema schema) {
        if(schema == null) {
            //TODO: log that the annotation is missing and return a default value
            System.out.println(field.getName() + " has null annotation");
            return Integer.MAX_VALUE;
        } else {
            return schema.order();
        }
    }

    // TODO: Figure out Array, Struct, and Map types.
    private static Optional<DataType> determineSimpleDataType(Class type) {
        if(ClassUtils.isAssignable(type, String.class, true)) { return Optional.of(DataTypes.StringType); }
        else if(type.isArray() && ClassUtils.isAssignable(type.getComponentType(), Byte.class, true)) { return Optional.of(DataTypes.BinaryType); }
        else if(ClassUtils.isAssignable(type, Boolean.class, true)) { return Optional.of(DataTypes.BooleanType); }
        else if(ClassUtils.isAssignable(type, Date.class, true)) { return Optional.of(DataTypes.DateType); }
        else if(ClassUtils.isAssignable(type, Timestamp.class, true)) { return Optional.of(DataTypes.TimestampType); }
        // else if(.class.isAssignableFrom(type)) { return Optional.of(DataTypes.CalendarIntervalType); }
        else if(ClassUtils.isAssignable(type, Double.class, true)) { return Optional.of(DataTypes.DoubleType); }
        else if(ClassUtils.isAssignable(type, Float.class, true)) { return Optional.of(DataTypes.FloatType); }
        else if(ClassUtils.isAssignable(type, Byte.class, true)) { return Optional.of(DataTypes.ByteType); }
        else if(ClassUtils.isAssignable(type, Integer.class, true)) { return Optional.of(DataTypes.IntegerType); }
        else if(ClassUtils.isAssignable(type, Long.class, true)) { return Optional.of(DataTypes.LongType); }
        else if(ClassUtils.isAssignable(type, Short.class, true)) { return Optional.of(DataTypes.ShortType); }
        else { return Optional.empty(); }
    }
}
