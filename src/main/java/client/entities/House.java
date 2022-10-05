package client.entities;


import annotations.Column;
import annotations.Entity;
import annotations.Id;
import annotations.OneToMany;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Entity
@AllArgsConstructor
@NoArgsConstructor
public class House {
    @Id
    @Getter
    @Setter
    private Long id;

    @Column
    @Getter
    @Setter
    private Integer number;

    @Getter
    @Setter
    @OneToMany(mappedBy = "house_id")
    private List<Person> persons = new ArrayList<>();

    public House(Long id, Integer number) {
        this.id = id;
        this.number = number;
    }
}