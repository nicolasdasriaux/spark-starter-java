package sparkstarter;

import java.util.Objects;

public class Pet {
    private int id;
    private String name;

    public Pet() {
    }

    public Pet(final int id, final String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(final int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Pet pet = (Pet) o;
        return id == pet.id &&
                Objects.equals(name, pet.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }
}
