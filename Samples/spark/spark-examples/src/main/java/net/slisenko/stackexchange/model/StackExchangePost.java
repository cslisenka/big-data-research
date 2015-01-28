package net.slisenko.stackexchange.model;

import java.io.Serializable;

public class StackExchangePost implements Serializable {

    private long id;
    private String title;
    private String text;

    public StackExchangePost() {
    }

    public StackExchangePost(long id, String text, String title) {
        this.text = text;
        this.id = id;
        this.title = title;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "StackExchangePost{" +
                "id=" + id +
                ", title='" + title + '\'' +
                ", text='" + text + '\'' +
                '}';
    }
}