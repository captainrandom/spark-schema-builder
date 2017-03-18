import org.apache.commons.lang3.ClassUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.Field;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Created by markjackson on 3/18/17.
 */
public class SchemaBuilder {

    public static <T> StructType determineSchema(Class<T> type) {
        List<StructField> sparkFieldList = new ArrayList<>();

        // TODO: Need some way of getting all fields (including the ones that are inherited)
        // TODO: Probably want to filter out protected/private fields that don't have any getters.
        for(Field field: type.getDeclaredFields()) {
            Optional<DataType> dataType = determineSimpleDataType(field.getType());
            if(!dataType.isPresent()) {
                // TODO: Need to find a better exception to throw there. Should be a non-handled exception type.
                throw new RuntimeException("Cant find corresponding type for " + field.getType() + ". I have yet to implement " +
                        "reflection for more complex types (ex: nested types).");
            }

            SparkSchema schemaAnnotation = field.getAnnotation(SparkSchema.class);
            boolean isNullable = schemaAnnotation == null || schemaAnnotation.isNullable();
            StructField structField = DataTypes.createStructField(field.getName(), dataType.get(), isNullable);
            sparkFieldList.add(structField);
        }
        return new StructType(sparkFieldList.toArray(new StructField[sparkFieldList.size()]));
    }

    // TODO: Figure out the rest of the data types and their corresponding mappings.
    public static Optional<DataType> determineSimpleDataType(Class type) {
        if(ClassUtils.isAssignable(type, String.class, true)) { return Optional.of(DataTypes.StringType); }
        // else if(.class.isAssignableFrom(type)) { return Optional.of(DataTypes.BinaryType); }
        else if(ClassUtils.isAssignable(type, Boolean.class, true)) { return Optional.of(DataTypes.BooleanType); }
        else if(ClassUtils.isAssignable(type, Date.class, true)) { return Optional.of(DataTypes.DateType); }
        // else if(.class.isAssignableFrom(type)) { return Optional.of(DataTypes.TimestampType); }
        // else if(.class.isAssignableFrom(type)) { return Optional.of(DataTypes.CalendarIntervalType); }
        else if(ClassUtils.isAssignable(type, Double.class, true)) { return Optional.of(DataTypes.DoubleType); }
        else if(ClassUtils.isAssignable(type, Float.class, true)) { return Optional.of(DataTypes.FloatType); }
        else if(ClassUtils.isAssignable(type, Byte.class, true)) { return Optional.of(DataTypes.ByteType); }
        else if(ClassUtils.isAssignable(type, Integer.class, true)) { return Optional.of(DataTypes.IntegerType); }
        else if(ClassUtils.isAssignable(type, Long.class, true)) { return Optional.of(DataTypes.LongType); }
        else if(ClassUtils.isAssignable(type, Short.class, true)) { return Optional.of(DataTypes.ShortType); }
        // else if(.class.isAssignableFrom(type)) { return Optional.of(DataTypes.NullType); }
        else { return Optional.empty(); }
    }
}
