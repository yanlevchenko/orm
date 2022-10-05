package orm.testsupport;

import org.h2.jdbcx.JdbcDataSource;
import org.jooq.CreateTableColumnStep;
import org.jooq.DSLContext;
import org.jooq.InsertSetStep;
import org.jooq.SQLDialect;
import org.jooq.SelectWhereStep;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.sql.SQLException;

import javax.sql.DataSource;

import static org.jooq.impl.DSL.table;

/**
 * Base integration method for test that need a database.
 */
public abstract class BaseIntegrationTest {
    protected DataSource dataSource;
    protected DSLContext dslContext;

    /**
     * Method that gets executed before each test method.
     */
    protected void beforeTestMethod() {
    }

    /**
     * Method that gets executed after each test method.
     */
    protected void afterTestMethod() {
    }

    @BeforeEach
    public final void before() {
        createDatabase();
        beforeTestMethod();
    }

    @AfterEach
    public final void after() throws SQLException {
        shutdownDatabase();
        afterTestMethod();
    }

    private void createDatabase() {
        var h2DataSource = new JdbcDataSource();
        h2DataSource.setURL("jdbc:h2:mem:test");
        h2DataSource.setUser("sa");
        h2DataSource.setPassword("");

        dataSource = h2DataSource;
        dslContext = DSL.using(this.dataSource, SQLDialect.H2);
    }

    private void shutdownDatabase() throws SQLException {
        try (var connection = dataSource.getConnection()) {
            try (var statement = connection.createStatement()) {
                statement.execute("DROP ALL OBJECTS DELETE FILES");
            }
        }
    }

    /**
     * Supportive method. Can be used to create tables in {@link #beforeTestMethod()}.
     * Refer to the JOOQ documentation on how to create tables. An example:
     * <pre>
     * {@code
     * createTable("Person")
     *     .column("id", BIGINT.identity(true))
     *     .column("firstName", VARCHAR(255))
     *     .column("lastName", VARCHAR(255))
     *     .column("dateOfBirth", DATE)
     *     .primaryKey("id")
     *     .execute()
     * }
     * </pre>
     *
     */
    protected final CreateTableColumnStep createTable(String tableName) {
        return dslContext.createTable(tableName);
    }

    /**
     * Supportive method. Can be used to initialize query data in test methods.
     * Refer to the JOOQ documentation on how to query data. An example:
     * <pre>
     * {@code
     * selectFrom("Person")
     *     .where(field("firstName").eq("Tomato"))
     *     .fetchOptional()
     * }
     * </pre>
     */
    protected final SelectWhereStep<?> selectFrom(String tableName) {
        return dslContext.selectFrom(tableName);
    }

    /**
     * Supportive method. Can be used to initialize tables with test data in {@link #beforeTestMethod()}.
     * Refer to the JOOQ documentation on how to insert data. An example:
     * <pre>
     * {@code
     * insertInto("Person")
     *     .set(field("firstName"), "Tomato")
     *     .set(field("lastName"), "Heinz")
     *     .set(field("dateOfBirth"), date("1980-07-24"))
     *     .execute()
     * }
     * </pre>
     */
    protected final InsertSetStep<?> insertInto(String tableName) {
        return dslContext.insertInto(table(tableName));
    }
}
