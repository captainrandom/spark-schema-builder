# spark-schema-builder
Why manually build a schema for a spark dataframe when you have already have everything defined in a model object?

This library helps build this schema from your model objects using reflection, so you don't have to (namely it gets rid
of boiler plate code).

Example:

StructType schema = SchemaBuilder.determineSchema(Model.class);

where your model might look something like this:

public class Model {
    private final String name;
    private final int catastrophicFailureCount;
    private final double moneyEarned;
    private Date date; // note that this must be a java.sql.Date
    
    public Model(String name, int catastrophicFailureCount, ...) { ... }
    
    ...
    
}