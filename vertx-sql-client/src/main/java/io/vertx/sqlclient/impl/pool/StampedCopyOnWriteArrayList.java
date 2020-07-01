// Copyright (C) 2017 Red Hat, Inc. and individual contributors as indicated by the @author tags.
// You may not use this file except in compliance with the Apache License, Version 2.0.

package io.vertx.sqlclient.impl.pool;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.lang.System.arraycopy;
import static java.lang.reflect.Array.newInstance;

/**
 * @author <a href="lbarreiro@redhat.com">Luis Barreiro</a>
 */
public final class StampedCopyOnWriteArrayList<T> implements List<T> {

    private final Iterator<T> emptyIterator = new Iterator<T>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }
    };

    private final StampedLock lock;
    private long optimisticStamp;
    private T[] data;

    // -- //

    @SuppressWarnings( "unchecked" )
    public StampedCopyOnWriteArrayList(Class<? extends T> clazz) {
        this.data = (T[]) newInstance( clazz, 0 );
        lock = new StampedLock();
        optimisticStamp = lock.tryOptimisticRead();
    }

    public T[] getUnderlyingArray() {
        T[] array = data;
        if ( lock.validate( optimisticStamp ) ) {
            return array;
        }

        // Acquiring a read lock does not increment the optimistic stamp
        long stamp = lock.readLock();
        try {
            return data;
        } finally {
            lock.unlockRead( stamp );
        }
    }

    @Override
    public T get(int index) {
        return getUnderlyingArray()[index];
    }

    @Override
    public int size() {
        return getUnderlyingArray().length;
    }

    // --- //

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public T set(int index, T element) {
        long stamp = lock.writeLock();
        try {
            T old = data[index];
            data[index] = element;
            return old;
        } finally {
            optimisticStamp = lock.tryConvertToOptimisticRead( stamp );
        }
    }

    @Override
    public boolean add(T element) {
        long stamp = lock.writeLock();
        try {
            data = Arrays.copyOf( data, data.length + 1 );
            data[data.length - 1] = element;
            return true;
        } finally {
            optimisticStamp = lock.tryConvertToOptimisticRead( stamp );
        }
    }

    public T removeLast() {
        long stamp = lock.writeLock();
        try {
            T element = data[data.length - 1];
            data = Arrays.copyOf( data, data.length - 1 );
            return element;
        } finally {
            optimisticStamp = lock.tryConvertToOptimisticRead( stamp );
        }
    }

    @Override
    public boolean remove(Object element) {
        long stamp = lock.readLock();
        try {
            boolean found = true;

            while ( found ) {
                T[] array = data;
                found = false;

                for ( int index = array.length - 1; index >= 0; index-- ) {
                    if ( element == array[index] ) {
                        found = true;

                        long writeStamp = lock.tryConvertToWriteLock( stamp );
                        if ( writeStamp != 0 ) {
                            stamp = writeStamp;

                            T[] newData = Arrays.copyOf( data, data.length - 1 );
                            arraycopy( data, index + 1, newData, index, data.length - index - 1 );
                            data = newData;
                            return true;
                        } else {
                            break;
                        }
                    }
                }
            }
            return false;
        } finally {
            optimisticStamp = lock.tryConvertToOptimisticRead( stamp );
        }
    }

    @Override
    public T remove(int index) {
        long stamp = lock.writeLock();
        try {
            T old = data[index];
            T[] array = Arrays.copyOf( data, data.length - 1 );
            if ( data.length - index - 1 != 0 ) {
                arraycopy( data, index + 1, array, index, data.length - index - 1 );
            }
            data = array;
            return old;
        } finally {
            optimisticStamp = lock.tryConvertToOptimisticRead( stamp );
        }
    }

    @Override
    public void clear() {
        long stamp = lock.writeLock();
        try {
            data = Arrays.copyOf( data, 0 );
        } finally {
            optimisticStamp = lock.tryConvertToOptimisticRead( stamp );
        }
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<T> iterator() {
        T[] array = getUnderlyingArray();
        return array.length == 0 ? emptyIterator : new UncheckedIterator<>( array );
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        long stamp = lock.writeLock();
        try {
            int oldSize = data.length;
            data = Arrays.copyOf( data, oldSize + c.size() );
            for ( T element : c ) {
                data[oldSize++] = element;
            }
            return true;
        } finally {
            optimisticStamp = lock.tryConvertToOptimisticRead( stamp );
        }
    }

    // --- //

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E> E[] toArray(E[] a) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<T> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<T> stream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<T> parallelStream() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        for ( T element : this ) {
            action.accept( element );
        }
    }

    @Override
    public Spliterator<T> spliterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeIf(Predicate<? super T> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void replaceAll(UnaryOperator<T> operator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void sort(Comparator<? super T> c) {
        throw new UnsupportedOperationException();
    }

    // --- //

    private static final class UncheckedIterator<T> implements Iterator<T> {

        private final int size;

        private final T[] data;

        private int index = 0;

        public UncheckedIterator(T[] data) {
            this.data = data;
            this.size = data.length;
        }

        @Override
        public boolean hasNext() {
            return index < size;
        }

        @Override
        public T next() {
            if ( index < size ) {
                return data[index++];
            }
            throw new NoSuchElementException( "No more elements in this list" );
        }
    }

}