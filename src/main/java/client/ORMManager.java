package client;

import annotations.Column;
import annotations.Entity;
import annotations.Id;
import annotations.ManyToOne;
import annotations.OneToMany;
import exceptions.ORMException;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.StringJoiner;

@Slf4j
public class ORMManager {
    private final Connection connection;

    private static final String FAILED_FIELD_GETTING_EXCEPTION = "Failed to get field.";
    private static final String STATEMENT_EXCEPTION = "SQLException occurred in the statement.";
    private static final String NO_FIELD_WITH_ID_ANNOTATION = "Entity has no field annotated with @Id";

    public ORMManager(String property) {
        try {
            Properties properties = readProperties();
            this.connection = DriverManager.getConnection(
                    properties.getProperty(property),
                    properties.getProperty("H2.username"),
                    properties.getProperty("H2.password")
            );
        } catch (SQLException e) {
            throw new ORMException("The connection was not established.", e);
        } catch (IOException e) {
            throw new ORMException("An exception occurred while reading the .properties file.", e);
        }
    }

    public ORMManager(Connection connection) {
        this.connection = connection;
    }

    public void prepareRepositoryFor(Class<?> clazz) {
        var classDesc = new ArrayList<Field>();

        if (clazz.isAnnotationPresent(Entity.class)) {
            for (var field : clazz.getDeclaredFields()) {
                field.setAccessible(true);
                classDesc.add(field);
            }
        } else {
            throw new IllegalArgumentException("Class must be marked with @Entity annotation.");
        }
        createTableFor(chooseType(classDesc), clazz.getSimpleName());
    }

    public void createTableFor(Map<Field, String> classDesc, String tableName) {
        var sql = sqlStatementForTableCreation(classDesc, tableName);

        try (var statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS `" + tableName + "`");
        } catch (SQLException throwable) {
            throw new ORMException(STATEMENT_EXCEPTION, throwable);
        }
        try (var preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
            log.info("Table has been created.");
        } catch (SQLException throwable) {
            throw new ORMException(STATEMENT_EXCEPTION, throwable);
        }
    }

    public Map<Field, String> chooseType(List<Field> classDesc) {
        var sqlClassDesc = new LinkedHashMap<Field, String>();
        var sqlType = "";
        for (var field : classDesc) {
            var javaType = field.getType().getSimpleName().toLowerCase(Locale.ROOT);
            if ("string".equals(javaType)) {
                sqlType = "VARCHAR(255)";
            } else if ("char".equals(javaType) || "character".equals(javaType)) {
                sqlType = "CHAR";
            } else if ("int".equals(javaType) || "integer".equals(javaType)) {
                sqlType = "INTEGER";
            } else if ("long".equals(javaType)) {
                sqlType = "BIGINT";
            } else if ("short".equals(javaType)) {
                sqlType = "SMALLINT";
            } else if ("byte".equals(javaType)) {
                sqlType = "TINYINT";
            } else if ("float".equals(javaType)) {
                sqlType = "REAL";
            } else if ("double".equals(javaType)) {
                sqlType = "DOUBLE";
            } else if ("boolean".equals(javaType)) {
                sqlType = "BOOLEAN";
            } else if ("date".equals(javaType) || "localdate".equals(javaType)) {
                sqlType = "DATE";
            } else if ("time".equals(javaType) || "localtime".equals(javaType)) {
                sqlType = "TIME";
            } else if ("timestamp".equals(javaType)) {
                sqlType = "TIMESTAMP";
            } else if ("offsetdatetime".equals(javaType)) {
                sqlType = "TIMESTAMP_WITH_TIMEZONE";
            } else if ("byte[]".equals(javaType)) {
                sqlType = "BINARY";
            } else if ("bigdecimal".equals(javaType)) {
                sqlType = "DECIMAL";
            } else if ("struct".equals(javaType)) {
                sqlType = "STRUCT";
            }
            sqlClassDesc.put(field, sqlType);
        }
        return sqlClassDesc;
    }

    public String sqlStatementForTableCreation(Map<Field, String> classDesc, String tableName) {
        var sql = new StringJoiner(", ", " (", ");");
        var constraints = new StringBuilder();
        for (var entry : classDesc.entrySet()) {
            var field = entry.getKey();
            var type = entry.getValue();

            if (field.isAnnotationPresent(Id.class)) {
                sql.add(field.getName() + " " + type + " AUTO_INCREMENT PRIMARY KEY");
            } else if (field.isAnnotationPresent(Column.class)) {
                sql.add(field.getName() + " " + type);
            }
            if (field.isAnnotationPresent(ManyToOne.class)) {
                sql.add(field.getAnnotation(ManyToOne.class).value() + " BIGINT");

                Field entityIdField = getEntityIdField(field.getType());
                sql.add(constraints.append(String.format(
                        "CONSTRAINT `%s_fk_1` FOREIGN KEY (`%s`) REFERENCES `%s` (`%s`)",
                        tableName,
                        field.getAnnotation(ManyToOne.class).value(),
                        field.getDeclaringClass().getSimpleName(),
                        entityIdField.getName() // primary key name
                )));
            }
        }
        return "CREATE TABLE " + tableName + sql;
    }

    public <T> void save(T entity) {
        if (!entity.getClass().isAnnotationPresent(Entity.class)) {
            throw new ORMException("This entity does not have the Entity annotation.", null);
        }
        if (isEntityPresentInDb(entity)) {
            throw new ORMException("Such an entity already exists in the DB.", null);
        }

        try (var statement = connection.createStatement()) {
            var sql = prepareInsertSqlStatementForSaving(entity);
            statement.execute(sql, Statement.RETURN_GENERATED_KEYS);

            try (var generatedKeys = statement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    Field entityIdField = getEntityIdField(entity.getClass());
                    entityIdField.setAccessible(true);
                    entityIdField.set(entity, generatedKeys.getLong(1));

                    log.info("{} has been saved with {} {}",
                            entity.getClass().getSimpleName(),
                            entityIdField.getName(),
                            generatedKeys.getString(1)
                    );
                } else {
                    throw new ORMException("Creating entity failed, no ID obtained.", null);
                }
            }
        } catch (SQLException throwable) {
            throw new ORMException(STATEMENT_EXCEPTION, throwable);
        } catch (IllegalAccessException e) {
            throw new ORMException(FAILED_FIELD_GETTING_EXCEPTION, e);
        }
    }

    public <T> void merge(T entity) {
        Field entityIdField = getEntityIdField(entity.getClass());

        if (isEntityPresentInDb(entity)) {
            var sql = prepareUpdateSqlStatementForMerging(entity, entityIdField);

            try (var prepareStatement = connection.prepareStatement(sql)) {
                var id = getEntityId(entity);
                prepareStatement.setLong(1, id);
                prepareStatement.executeUpdate();

                log.info("{} has been merged.",
                        entity.getClass().getSimpleName()
                );
            } catch (SQLException e) {
                throw new ORMException(STATEMENT_EXCEPTION, e);
            }

        } else {
            throw new ORMException("There is no such entity in the database.", new NoSuchElementException());
        }
    }

    public <T> T getById(Class<T> clazz, Long id) {
        String sql = prepareSelectSqlStatementForGettingEntityById(clazz);

        try (var statement = connection.prepareStatement(sql)) {
            statement.setLong(1, id);
            try (var resultSet = statement.executeQuery()) {
                if (resultSet.next()) {

                    var o = (T) convertResultSetRowToJavaObject(clazz, resultSet);
                    fetchRelations(o);
                    return o;
                }
            }
        } catch (SQLException e) {
            throw new ORMException(STATEMENT_EXCEPTION, e);
        }
        return null;
    }

    private <T> String prepareSelectSqlStatementForGettingEntityById(Class<T> clazz) {
        var selectBody = getSelectBodyForDbRequest(clazz);
        return "SELECT " + String.join(", ", selectBody) + " FROM " +
                clazz.getSimpleName() + " WHERE " + getEntityIdField(clazz).getName() + "= ?";
    }

    public <T> List<T> getAll(Class<T> clazz) {
        String sql = prepareSelectSqlStatementForGettingAllEntities(clazz);
        var allObjects = new ArrayList<T>();
        try (var statement = connection.prepareStatement(sql)) {
            try (var resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    var o = (T) convertResultSetRowToJavaObject(clazz, resultSet);
                    fetchRelations(o);
                    allObjects.add(o);
                }
                return allObjects;
            }
        } catch (SQLException e) {
            throw new ORMException(STATEMENT_EXCEPTION, e);
        }
    }

    private <T> String prepareSelectSqlStatementForGettingAllEntities(Class<T> clazz) {
        var selectBody = getSelectBodyForDbRequest(clazz);
        return "SELECT " + String.join(", ", selectBody) + " FROM " +
                clazz.getSimpleName();
    }

    private <T> void fetchRelations(T entity) {
        try {
            for (var field : entity.getClass().getDeclaredFields()) {
                field.setAccessible(true);

                if (field.isAnnotationPresent(OneToMany.class) &&
                        List.class.isAssignableFrom(field.getType())) {
                    Class<?> objectsType = getTypeOfListObjects(field);

                    String getListOfObjects =
                            prepareSelectStatementForGettingListOfManyToOneObjects(
                                    entity, field, objectsType
                            );

                    try (var pstmt = connection.prepareStatement(getListOfObjects);
                         var resultSet = pstmt.executeQuery()) {

                        var newListWithObjectsFromDb = new ArrayList<>();
                        while (resultSet.next()) {
                            Object newObject = convertResultSetRowToJavaObject(objectsType, resultSet);
                            fetchRelations(newObject);

                            newListWithObjectsFromDb.add(newObject);
                        }
                        field.set(entity, newListWithObjectsFromDb);
                    }
                }
                if (field.isAnnotationPresent(ManyToOne.class)) {
                    Class<?> oneToManyObjectType = field.getType();

                    String getOneToManyObject =
                            prepareSelectSqlStatementForGettingOneToManyObject(
                                    entity, field, oneToManyObjectType
                            );

                    try (var pstmt = connection.prepareStatement(getOneToManyObject);
                         var resultSet = pstmt.executeQuery()) {
                        if (resultSet.next()) {
                            field.set(entity, convertResultSetRowToJavaObject(oneToManyObjectType, resultSet));
                        }
                    }
                }
            }
            log.info("In {} object relations have been fetched", entity);
        } catch (SQLException e) {
            throw new ORMException(STATEMENT_EXCEPTION, e);
        } catch (IllegalAccessException e) {
            throw new ORMException(FAILED_FIELD_GETTING_EXCEPTION, e);
        }
    }

    private <T> String prepareSelectStatementForGettingListOfManyToOneObjects(
            T entity,
            Field field,
            Class<?> objectsType) {

        Long entityId = getEntityId(entity);

        List<String> selectBody = getSelectBodyForDbRequest(objectsType);
        return "SELECT " + String.join(", ", selectBody) +
                " FROM " +
                objectsType.getSimpleName() +
                " WHERE " + objectsType.getSimpleName() + "." +
                field.getAnnotation(OneToMany.class).mappedBy() + "=" + entityId;
    }

    private <T> String prepareSelectSqlStatementForGettingOneToManyObject(
            T entity,
            Field field,
            Class<?> oneToManyObjectType) throws IllegalAccessException {

        Field entityIdField = getEntityIdField(entity.getClass());
        entityIdField.setAccessible(true);

        List<String> selectBody = getSelectBodyForDbRequest(oneToManyObjectType);
        return "SELECT " + String.join(", ", selectBody) +
                " FROM " +
                entity.getClass().getSimpleName() + " INNER JOIN " +
                oneToManyObjectType.getSimpleName() + " ON " +
                entity.getClass().getSimpleName() +
                "." + field.getAnnotation(ManyToOne.class).value() + "=" +
                oneToManyObjectType.getSimpleName() +
                "." + getEntityIdField(oneToManyObjectType).getName() +
                " WHERE " + entity.getClass().getSimpleName() + "." +
                entityIdField.getName() + "=" + entityIdField.get(entity);
    }

    private Object convertResultSetRowToJavaObject(Class<?> objectType, ResultSet resultSet) {
        try {
            Object newInstance = Class.forName(objectType.getName())
                    .getConstructor()
                    .newInstance();

            for (var field : newInstance.getClass().getDeclaredFields()) {
                if (field.isAnnotationPresent(Column.class) || field.isAnnotationPresent(Id.class)) {
                    field.setAccessible(true);
                    Object columnValue = resultSet.getObject(field.getName());

                    if (columnValue instanceof Date) {
                        columnValue = ((Date) columnValue).toLocalDate();
                    }

                    field.set(newInstance, columnValue);
                }
            }

            return newInstance;

        } catch (NoSuchMethodException e) {
            throw new ORMException("Failed to find such constructor", e);
        } catch (InvocationTargetException e) {
            throw new ORMException("Failed to invoke constructor or class method", e);
        } catch (InstantiationException e) {
            throw new ORMException("Failed to create class using newInstance method", e);
        } catch (IllegalAccessException e) {
            throw new ORMException(FAILED_FIELD_GETTING_EXCEPTION, e);
        } catch (SQLException e) {
            throw new ORMException(STATEMENT_EXCEPTION, e);
        } catch (ClassNotFoundException e) {
            throw new ORMException("Class was not found.", e);
        }
    }

    private List<String> getSelectBodyForDbRequest(Class<?> objectType) {
        var objectFields = new ArrayList<String>();
        for (Field objectField : objectType.getDeclaredFields()) {
            if (objectField.isAnnotationPresent(Column.class)
                    || objectField.isAnnotationPresent(Id.class)) {

                objectFields.add(objectField.getDeclaringClass().getSimpleName() + "." +
                        objectField.getName());
            }
        }
        return objectFields;
    }

    private Class<?> getTypeOfListObjects(Field field) {
        var listOfObjects = (ParameterizedType) field.getGenericType();
        return (Class<?>) listOfObjects.getActualTypeArguments()[0];
    }

    public void print(Class<?> clazz) {
        if (!clazz.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException();
        }
        try (var statement = connection.createStatement()) {
            var sql = prepareSelectSqlStatementForPrinting(clazz);
            try (var resultSet = statement.executeQuery(sql)) {
                var rows = new ArrayList<List<String>>();
                //adding each fields name to the Header Joiner
                var namesOfFields = Arrays.stream(clazz.getDeclaredFields())
                        .filter(field -> !field.isAnnotationPresent(OneToMany.class))
                        .map(Field::getName)
                        .collect(Collectors.toList());
                rows.add(namesOfFields);
                while (resultSet.next()) {
                    var row = new ArrayList<String>();
                    for (var i = 1; i <= namesOfFields.size(); i++) {
                        row.add(resultSet.getString(i));
                    }
                    rows.add(row);
                }
                System.out.println(generateOutputString(rows));
                log.info("Table {} has been printed.", clazz.getSimpleName());
            } catch (SQLException throwable) {
                throw new ORMException(STATEMENT_EXCEPTION, throwable);
            }
        } catch (SQLException throwable) {
            throw new ORMException(STATEMENT_EXCEPTION, throwable);
        }
    }

    public <T> void delete(T entity) {
        if (isEntityPresentInDb(entity)) {
            var sql = prepareSqlStatementForDeleting(entity);
            try (var prepareStatement = connection.prepareStatement(sql)) {
                prepareStatement.executeUpdate();

                log.info("{} has been deleted.", entity.getClass().getSimpleName());
            } catch (SQLException e) {
                throw new ORMException(STATEMENT_EXCEPTION, e);
            }
        } else {
            throw new ORMException("There is no such entity in the database.", null);
        }
    }

    private <T> String prepareInsertSqlStatementForSaving(T entity) {
        var fieldValues = new ArrayList<String>();

        getEntityFieldsExceptId(entity.getClass())
                .filter(field ->
                        field.isAnnotationPresent(Column.class) || field.isAnnotationPresent(ManyToOne.class))
                .forEach(field -> {
                    try {
                        field.setAccessible(true);

                        if (field.get(entity) == null) {
                            fieldValues.add("NULL");
                        } else if (field.isAnnotationPresent(ManyToOne.class)) {
                            Object annotatedEntity = field.get(entity);
                            fieldValues.add("'" + getEntityId(annotatedEntity) + "'");
                        } else {
                            fieldValues.add("'" + field.get(entity) + "'");
                        }

                    } catch (IllegalAccessException e) {
                        throw new ORMException(FAILED_FIELD_GETTING_EXCEPTION, e);
                    }
                });

        return "insert into " +
                entity.getClass().getSimpleName() +
                " values(" + "DEFAULT, " +
                String.join(", ", fieldValues) +
                ")";
    }

    private <T> String prepareUpdateSqlStatementForMerging(T entity, Field entityId) {
        List<String> fields = new ArrayList<>();

        getEntityFieldsExceptId(entity.getClass())
                .filter(field -> field.isAnnotationPresent(Column.class))
                .forEach(field -> {
                    try {
                        field.setAccessible(true);
                        fields.add(field.getName() + " = " + "'" + field.get(entity) + "'");
                    } catch (IllegalAccessException e) {
                        throw new ORMException(FAILED_FIELD_GETTING_EXCEPTION, e);
                    }
                });

        return "UPDATE " +
                entity.getClass().getSimpleName() +
                " SET " + String.join(",", fields) +
                " WHERE " + entityId.getName() + " = ?";
    }

    private String prepareSelectSqlStatementForPrinting(Class<?> clazz) {
        return "SELECT * FROM " +
                clazz.getSimpleName();
    }

    private <T> String prepareSqlStatementForDeleting(T entity) {
        Field entityIdField = getEntityIdField(entity.getClass());

        var sql = new StringBuilder("DELETE FROM ");
        entityIdField.setAccessible(true);
        try {
            sql.append(entity.getClass().getSimpleName())
                    .append(" WHERE ")
                    .append(entityIdField.getName())
                    .append(" = '")
                    .append(entityIdField.get(entity))
                    .append("'");

        } catch (IllegalAccessException e) {
            throw new ORMException(FAILED_FIELD_GETTING_EXCEPTION, e);
        }
        return sql.toString();
    }


    private <T> boolean isEntityPresentInDb(T entity) {
        Field entityIdField = getEntityIdField(entity.getClass());

        var sql = new StringBuilder();
        entityIdField.setAccessible(true);
        try {
            sql.append("SELECT EXISTS(SELECT * FROM ")
                    .append(entity.getClass().getSimpleName())
                    .append(" WHERE ")
                    .append(entityIdField.getName())
                    .append(" = ")
                    .append(entityIdField.get(entity))
                    .append(")");
        } catch (IllegalAccessException e) {
            throw new ORMException(FAILED_FIELD_GETTING_EXCEPTION, e);
        }

        boolean isEntityPresentInDb;
        try (
                var stmt = connection.createStatement();
                var rs = stmt.executeQuery(sql.toString())
        ) {
            rs.next();
            isEntityPresentInDb = rs.getBoolean(1);
        } catch (
                SQLException e) {
            throw new ORMException(STATEMENT_EXCEPTION, e);
        }
        return isEntityPresentInDb;
    }

    private Field getEntityIdField(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Id.class))
                .findFirst().orElseThrow(() ->
                        new ORMException(NO_FIELD_WITH_ID_ANNOTATION, new NoSuchFieldException()));
    }

    private Long getEntityId(Object entity) {
        try {
            Field entityIdField = getEntityIdField(entity.getClass());
            entityIdField.setAccessible(true);

            return (Long) entityIdField.get(entity);
        } catch (IllegalAccessException e) {
            throw new ORMException(FAILED_FIELD_GETTING_EXCEPTION, e);
        }
    }

    private Stream<Field> getEntityFieldsExceptId(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .filter(field -> !field.isAnnotationPresent(Id.class));
    }

    private Properties readProperties() throws IOException {
        var properties = new Properties();
        try (var fis = ORMManager.class.getClassLoader().getResourceAsStream("db.properties")) {
            properties.load(fis);
        }
        return properties;
    }

    public void closeConnection() {
        try {
            connection.close();
        } catch (SQLException e) {
            throw new ORMException("SQLException occurred while closing the connection.", e);
        }
    }

    private String generateOutputString(List<List<String>> rows) {
        var maxLengths = findMaxLengthOfWordsForEachField(rows);
        rows = makeWordsTheSameLength(rows, maxLengths);
        return mergeAllWordsToRows(rows);
    }

    private List<List<String>> makeWordsTheSameLength(List<List<String>> rows, List<Integer> maxLengths) {
        for (var i = 0; i < rows.size(); i++) {
            for (var j = 0; j < rows.get(i).size(); j++) {
                var word = rows.get(i).get(j);
                if (word.length() < maxLengths.get(j)) {
                    rows.get(i).set(j, increaseLengthOfWord(word, maxLengths.get(j)));
                }
            }
        }
        return rows;
    }

    private String mergeAllWordsToRows(List<List<String>> rows) {
        var outputBuilder = new StringBuilder();
        for (var row : rows) {
            var stringJoinerToRow = new StringJoiner(" | ", "| ", " |");
            for (String s : row) {
                stringJoinerToRow.add(s);
            }
            outputBuilder.append(stringJoinerToRow + "\n");
        }
        return outputBuilder.toString();
    }

    private String increaseLengthOfWord(String word, int neededLength) {
        var neededPartLength = neededLength - word.length();
        var newWord = new StringBuilder(word);
        for (var i = 0; i < neededPartLength; i++) {
            newWord.append(" ");
        }
        return newWord.toString();
    }

    private List<Integer> findMaxLengthOfWordsForEachField(List<List<String>> rows) {
        var maxLengths = new ArrayList<Integer>();
        var max = 0;
        for (var i = 0; i < rows.get(0).size(); i++) {
            for (var j = 0; j < rows.size(); j++) {
                if (Objects.isNull(rows.get(j).get(i))) {
                    rows.get(j).set(i, "null");
                }
                if (max < rows.get(j).get(i).length()) {
                    max = rows.get(j).get(i).length();
                }
            }
            maxLengths.add(max);
            max = 0;
        }
        return maxLengths;
    }
}
