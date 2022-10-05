package client;

import client.entities.Author;
import client.entities.Book;
import client.entities.House;
import client.entities.Person;
import java.time.LocalDate;

public class Application {
    public static void main(String[] args) {
        var ormManager = new ORMManager("H2.url");
//        ormManager.prepareRepositoryFor(Person.class);
//        ormManager.prepareRepositoryFor(House.class);

//        House house = new House();
//        house.setNumber(1);
//        ormManager.save(house);
//
//        var person1 = new Person("Yan", "Levchenko", LocalDate.now(), house);
//        var person2 = new Person("Garik", "Evselov", LocalDate.now(), house);
//
//        ormManager.save(person1);
//        ormManager.save(person2);
//
//        person1.setFirstName("Mark");
//        ormManager.merge(person1);
//
//        ormManager.update(house);
//        System.out.println(house.getPersons());


        ormManager.prepareRepositoryFor(Author.class);
        ormManager.prepareRepositoryFor(Book.class);

        Author author1 = new Author("Yan");
        ormManager.save(author1);

        Author author2 = new Author("Mark");
        ormManager.save(author2);

        Book book1 = new Book("Sumerki", "Love", LocalDate.now(), author1);
        Book book2 = new Book("Surviver", "Adventures", LocalDate.now(), author1);
        Book book3 = new Book("Harry Potter", null, null);

        ormManager.save(book1);
        ormManager.save(book2);
        ormManager.save(book3);

        var allAuthors = ormManager.getAll(Author.class);
        var allBooks = ormManager.getAll(Book.class);
        System.out.println("all authors:" + allAuthors + "\n");
        System.out.println("all books:" + allBooks + "\n");

        Author updatedAuthor1 = ormManager.getById(Author.class, author1.getId());
        Author updatedAuthor2 = ormManager.getById(Author.class, author2.getId());
//        ormManager.update(author2);

//        Author newAuthor = ormManager.getById(Author.class, 1L);
//        System.out.println(newAuthor.getBooks());
//
//        System.out.println(newAuthor);
        System.out.println(updatedAuthor1.getBooks());
        System.out.println(updatedAuthor2.getBooks());

        System.out.println(ormManager.getById(Book.class, 3L));

        ormManager.print(Author.class);
        ormManager.print(Book.class);

        ormManager.closeConnection();
    }
}
