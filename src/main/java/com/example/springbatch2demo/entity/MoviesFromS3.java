package com.example.springbatch2demo.entity;

import jakarta.persistence.*;

@Entity
@Table(name = "MOVIES_FROM_S3", schema="test-db")
public class MoviesFromS3 {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "name")
    private String name;

    @Column(name = "genre")
    private String genre;

    @Column(name = "releaseYear")
    private int releaseYear;

    @Column(name = "releasePlatform")
    private String releasePlatform;


    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    public int getReleaseYear() {
        return releaseYear;
    }

    public void setReleaseYear(int releaseYear) {
        this.releaseYear = releaseYear;
    }

    public String getReleasePlatform() {
        return releasePlatform;
    }

    public void setReleasePlatform(String releasePlatform) {
        this.releasePlatform = releasePlatform;
    }

    public MoviesFromS3(Long id, String name, String genre, int releaseYear, String releasePlatform) {
        this.id = id;
        this.name = name;
        this.genre = genre;
        this.releaseYear = releaseYear;
        this.releasePlatform = releasePlatform;
    }

    public MoviesFromS3() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MoviesFromS3 that = (MoviesFromS3) o;

        if (releaseYear != that.releaseYear) return false;
        if (!id.equals(that.id)) return false;
        if (!name.equals(that.name)) return false;
        if (!genre.equals(that.genre)) return false;
        return releasePlatform.equals(that.releasePlatform);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + genre.hashCode();
        result = 31 * result + releaseYear;
        result = 31 * result + releasePlatform.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MoviesFromS3{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", genre='" + genre + '\'' +
                ", releaseYear=" + releaseYear +
                ", releasePlatform='" + releasePlatform + '\'' +
                '}';
    }
}
