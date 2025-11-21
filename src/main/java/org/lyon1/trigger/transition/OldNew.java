package org.lyon1.trigger.transition;

public final class OldNew<T> {
    private final T old;
    private final T nnew; // cannot use 'new' as a field name

    public OldNew(T old, T nnew) {
        this.old = old;
        this.nnew = nnew;
    }

    public T old() {
        return old;
    }

    public T nnew() {
        return nnew;
    }
}
