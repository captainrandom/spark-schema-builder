# spark-schema-builder
Why manually build a schema for a spark dataframe when you have already have everything defined in a model object?

This library helps build this schema from your model objects using reflection, so you don't have to (namely it gets rid
of boiler plate code).

###Examples:

####How to create the schema:

`StructType schema = SchemaBuilder.determineSchema(Model.class);`

####where your model might look something like this:
Please note:
* The model object must be a bean "compliant"
* aka must have a default constructor (can have other constructors)
* needs to be serializable
* All setters need to return void
```java
public class Model implements Serializable {
    private final String name;
    private final int catastrophicFailureCount;
    private final double moneyEarned;
    private Date date; // note that this must be a java.sql.Date
    
    public Model() {} // a default constructor is necessary
    
    public Model(String name, int catastrophicFailureCount, double moneyEarned, Date date) {
        this.name = name;
        this.catastrophicFailureCount = catastrophicFailureCount;
        this.moneyEarned = moneyEarned;
        this.date = date;
    }
    ...
}
```

##Features still to be Added/Considered

* Handle Arrays, Maps, Struct, CalendarIntervalType, and complex user defined objects.
* Need to do further consider adding more annotations to ignore fields
* Currently, all fields on the object are included in the schema. Might consider determining which fields are not accessible through a getter and filter out those from the schema.