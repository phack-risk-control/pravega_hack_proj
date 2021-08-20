package io.phack;
import java.io.Serializable;

public class WordCount implements Serializable {

    public String word;
    public int count;

    public WordCount() {}

    public WordCount(String word, int count) {
        this.count = count;
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getWord() {return word;}

    public void setWord(String word) {this.word = word;}

    @Override
    public String toString() {
        return "Word: "+ word + ": " + " Count: " + count;
    }
}