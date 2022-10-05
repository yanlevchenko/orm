package client.entities;

import annotations.Column;
import annotations.Entity;
import annotations.Id;
import annotations.ManyToOne;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;

@Entity
@AllArgsConstructor
@NoArgsConstructor
public class Book {
    @Id
    @Getter
    @Setter
    private Long id;

    @Column
    @Getter
    @Setter
    private String name;

    @Column
    @Getter
    @Setter
    private String genre;

    @Column
    @Getter
    @Setter
    private LocalDate dateOfWriting;

    @Getter @Setter
    @ManyToOne("author_id")
    private Author author;

    public Book(Long id, String name, String genre, LocalDate dateOfWriting) {
        this.id = id;
        this.name = name;
        this.genre = genre;
        this.dateOfWriting = dateOfWriting;
    }

    public Book(String name, String genre, LocalDate dateOfWriting) {
        this.name = name;
        this.genre = genre;
        this.dateOfWriting = dateOfWriting;
    }

    public Book(String name, String genre, LocalDate dateOfWriting, Author author) {
        this.name = name;
        this.genre = genre;
        this.dateOfWriting = dateOfWriting;
        this.author = author;
    }

    @Override
    public String toString() {
        return "Book{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", genre='" + genre + '\'' +
                ", dateOfWriting=" + dateOfWriting +
                ", author=" + author +
                '}';
    }
}