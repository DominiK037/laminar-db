package io.laminar.core.model;

import java.util.Arrays;

/**
 * Content-addressable wrapper around a raw key byte array.
 *
 * <p>{@code byte[]} cannot be used directly as a {@link java.util.HashMap} key because
 * its {@code hashCode()} and {@code equals()} are based on reference identity, not
 * content. Two arrays read from disk with identical bytes would be treated as
 * different keys, making the index useless.
 *
 * <p>{@code BytesKey} overrides both methods using {@link Arrays#hashCode} and
 * {@link Arrays#equals}, giving content-based equality. The wrapper is a record
 * for immutability and compact syntax, but the overrides are explicit because
 * the default record implementations delegate to {@code byte[].hashCode()} and
 * {@code byte[].equals()}, which retain reference semantics.
 *
 * <p><b>Why no defensive copy:</b> keys entering the index are freshly allocated
 * from disk reads and are never mutated after wrapping. Copying every key would
 * double allocation on index rebuild with no safety benefit.
 *
 * <p><b>Thread safety:</b> immutable after construction. Safe to use as a map
 * key from any thread.
 */
public record BytesKey(byte[] data) {

    /**
     * Content-based hash — delegates to {@link Arrays#hashCode(byte[])}.
     *
     * <p>Overrides the default record implementation, which would call
     * {@code data.hashCode()} and return a reference-identity hash.
     *
     * @return hash code derived from the content of {@link #data}
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(this.data);
    }

    /**
     * Content-based equality — delegates to {@link Arrays#equals(byte[], byte[])}.
     *
     * <p>Overrides the default record implementation, which would call
     * {@code data.equals(other.data)} and compare by reference.
     *
     * @param obj the object to compare
     * @return {@code true} if {@code obj} is a {@code BytesKey} whose
     *         {@link #data} array has identical length and content
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof BytesKey other)) return false;
        return Arrays.equals(this.data, other.data);
    }
}
