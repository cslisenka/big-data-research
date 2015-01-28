package net.slisenko.stackexchange.util;

import java.util.Iterator;

public abstract class AbstractIterableParser<T> implements Iterable<T> {

    private Iterator<String> iterator;

    public AbstractIterableParser(Iterator<String> iterator) {
        this.iterator = iterator;
    }

    public abstract T parse(String string);

    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return parse(iterator.next());
            }

            @Override
            public void remove() {
                throw new RuntimeException("Method not supported");
            }
        };
    }
}