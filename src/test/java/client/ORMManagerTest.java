package client;

import client.entities.Author;
import client.entities.Book;
import client.entities.Person;
import exceptions.ORMException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import orm.testsupport.BaseIntegrationTest;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;


class ORMManagerTest extends BaseIntegrationTest {

    @Test
    @DisplayName("Should succeed when the number of rows is equal to one row in the db table")
    void givenNewPerson_save_shouldSaveThePerson() throws SQLException {
        ORMManager ormManager = new ORMManager(dataSource.getConnection());

        ormManager.prepareRepositoryFor(Person.class);
        var person = new Person("Yan", "Levchenko", LocalDate.now());
        ormManager.save(person);

        assertEquals(1, selectFrom("PERSON")
                .where("PERSON.id = 1")
                .stream().count());
    }

    @Test
    @DisplayName("Should succeed when such entity already exists in the db table")
    void givenNewPerson_save_shouldSaveThePersonAndThrowException() throws SQLException {
        ORMManager ormManager = new ORMManager(dataSource.getConnection());

        ormManager.prepareRepositoryFor(Person.class);
        Person person = new Person("Yan", "Levchenko", LocalDate.now());

        ormManager.save(person);

        assertThrows(ORMException.class, () -> ormManager.save(person));
    }

    @Test
    @DisplayName("Should succeed when the firstname is Mark in the db table")
    void givenNewPerson_saveAndMerge_shouldSaveThePersonAndAssignFirstnameToMarkAndMergeIt() throws SQLException {
        ORMManager ormManager = new ORMManager(dataSource.getConnection());

        ormManager.prepareRepositoryFor(Person.class);
        Person person = new Person("Yan", "Levchenko", LocalDate.now());
        ormManager.save(person);

        person.setFirstName("Mark");
        ormManager.merge(person);

        assertEquals(1, selectFrom("PERSON")
                .where("PERSON.id = 1")
                .and("PERSON.firstname = 'Mark'")
                .stream().count());
    }

    @Test
    @DisplayName("Should succeed when no such entity exists")
    void givenNewPerson_merge_shouldMergeThePersonAndThrowException() throws SQLException {
        ORMManager ormManager = new ORMManager(dataSource.getConnection());

        ormManager.prepareRepositoryFor(Person.class);
        Person person = new Person("Yan", "Levchenko", LocalDate.now());

        assertThrows(ORMException.class, () -> ormManager.merge(person));
    }

    @Test
    @DisplayName("Should succeed when updated author has 2 entries in the book list")
    void givenAuthorAndTwoBooks_saveAndGetById_shouldSaveAuthorAndTwoBooksAndGetUpdatedAuthorWithTwoBooks() throws SQLException {
        ORMManager ormManager = new ORMManager(dataSource.getConnection());

        ormManager.prepareRepositoryFor(Author.class);
        ormManager.prepareRepositoryFor(Book.class);

        Author author = new Author("Yan");
        ormManager.save(author);

        Book book1 = new Book("Sumerki", "Love", LocalDate.now(), author);
        Book book2 = new Book("Surviver", "Adventures", LocalDate.now(), author);

        ormManager.save(book1);
        ormManager.save(book2);

        Author updatedAuthor = ormManager.getById(Author.class, author.getId());

        assertEquals(2, updatedAuthor.getBooks().size());
    }

    @Test
    @DisplayName("Should succeed when gotten book has not author")
    void givenBook_saveAndGetById_shouldSaveBookAndGetBookByIdWithoutAuthor() throws SQLException {
        ORMManager ormManager = new ORMManager(dataSource.getConnection());

        ormManager.prepareRepositoryFor(Author.class);
        ormManager.prepareRepositoryFor(Book.class);

        Book book = new Book("Surviver", "Adventures", LocalDate.now());

        ormManager.save(book);

        Book dbBook = ormManager.getById(Book.class, 1L);

        assertNull(dbBook.getAuthor());
    }

    @Test
    @DisplayName("Should succeed when no such entity exists and return null")
    void givenBookId_getById_shouldGetBookByIdAndReturnNull() throws SQLException {
        ORMManager ormManager = new ORMManager(dataSource.getConnection());

        ormManager.prepareRepositoryFor(Author.class);
        ormManager.prepareRepositoryFor(Book.class);

        assertNull(ormManager.getById(Book.class, 1L));
    }

    @Test
    @DisplayName("Should succeed after checking if the deleted entity is not present in db")
    void givenTwoExistingPersons_delete_shouldDeleteOnePerson() throws SQLException {
        ORMManager ormManager = new ORMManager(dataSource.getConnection());
        ormManager.prepareRepositoryFor(Person.class);

        Person Yan = new Person("Yan", "Levchenko", LocalDate.now());
        Person Oksana = new Person("Oksana", "Linnyk", LocalDate.now().minusYears(6));

        ormManager.save(Yan);
        ormManager.save(Oksana);

        ormManager.delete(Yan);

        assertEquals(0, selectFrom("PERSON")
                .where("PERSON.firstname = 'Yan'")
                .stream().count());
    }

    @Test
    @DisplayName("Should succeed when gotten list of authors has size 2 and have the same values with parent object")
    void givenTwoAuthors_saveAndGetAll_ShouldSaveAuthorAndTwoBooksAndGetAll() throws SQLException {
        ORMManager ormManager = new ORMManager(dataSource.getConnection());

        ormManager.prepareRepositoryFor(Author.class);
        ormManager.prepareRepositoryFor(Book.class);

        Author author1 = new Author("Anton Martynenko");
        ormManager.save(author1);

        Author author2 = new Author("Yan Levchenko");
        ormManager.save(author2);

        List<Author> authors = ormManager.getAll(Author.class);

        assertEquals(2, authors.size());
        assertEquals(author1.getId(), authors.get(0).getId());
        assertEquals(author2.getName(), authors.get(1).getName());
    }
}