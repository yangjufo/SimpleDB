package simpledb;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(final Type t, final String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public final String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private final List<TDItem> items = new ArrayList<>();
    private int size;

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public final Iterator<TDItem> iterator() {
        return items.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(final Type[] typeAr, final String[] fieldAr) {
        for (int i = 0; i < typeAr.length; i++) {
            items.add(new TDItem(typeAr[i], fieldAr[i]));
            size += typeAr[i].getLen();
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(final Type[] typeAr) {
        for (final Type type : typeAr) {
            items.add(new TDItem(type, ""));
            size += type.getLen();
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return items.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(final int i) throws NoSuchElementException {
        if (i > numFields()) {
            throw new NoSuchElementException();
        }
        return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(final int i) throws NoSuchElementException {
        if (i > numFields()) {
            throw new NoSuchElementException();
        }
        return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(final String name) throws NoSuchElementException {
        for (int i = 0; i < numFields(); i++) {
            if (items.get(i).fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(final TupleDesc td1, final TupleDesc td2) {
        final Type[] typeAr = Stream.of(td1.items, td2.items).flatMap(Collection::stream).map(x -> x.fieldType)
                .toArray(Type[]::new);
        final String[] fieldAr = Stream.of(td1.items, td2.items).flatMap(Collection::stream).map(x -> x.fieldName)
                .toArray(String[]::new);
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TupleDesc tupleDesc = (TupleDesc) o;
        if (numFields() != tupleDesc.numFields() || getSize() != tupleDesc.getSize()) {
            return false;
        }
        for (int i = 0; i < numFields(); i++) {
            if (!getFieldType(i).equals(tupleDesc.getFieldType(i))) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        return items.stream().limit(items.size()).map(TDItem::toString).collect(Collectors.joining(","));
    }
}
