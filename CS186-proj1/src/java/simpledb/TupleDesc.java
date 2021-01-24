package simpledb;

import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    public static final String UN_NAMED_FIELD = "FIELD";
    private static final long serialVersionUID = 1L;
    // num of items in the table
    private int numOfFields;
    // fields in the table
    private TDItem[] items;
    // if named fields
    private boolean anonymous = false;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        fillTupleDesc(typeAr, fieldAr);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        fillTupleDesc(typeAr, new String[0]);
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        // TODO: can I just merge item and anonymous?
        int mergeNumOfFields = td1.numFields() + td2.numFields();
        String[] mergeNames = new String[mergeNumOfFields];
        Type[] mergeType = new Type[mergeNumOfFields];
        int count = 0;
        Iterator<TDItem> td1Iterator = td1.iterator();
        Iterator<TDItem> td2Iterator = td2.iterator();
        while (td1Iterator.hasNext()) {
            TDItem item = td1Iterator.next();
            mergeType[count] = item.fieldType;
            mergeNames[count] = item.fieldName;
            count++;
        }
        while (td2Iterator.hasNext()) {
            TDItem item = td2Iterator.next();
            mergeType[count] = item.fieldType;
            mergeNames[count] = item.fieldName;
            count++;
        }
        boolean mergeAnonymous = td1.anonymous & td2.anonymous;
        if (mergeAnonymous) {
            return new TupleDesc(mergeType);
        }
        return new TupleDesc(mergeType, mergeNames);
    }

    private void fillTupleDesc(Type[] typeAr, String[] fieldAr) {
        int typeSize = typeAr.length;
        // check size for typeAr
        if (typeSize < 1) {
            // throw new DbException("must has one element at least");
        }
        // check null for fieldAt
        // TODO: this check may be unnecessary, cause null or empty name is ok
        long nonEmptyNameSize = Stream.of(fieldAr).filter(name -> null != name && !name.isEmpty()).count();
        // check size for fieldAr
        if (fieldAr.length != typeSize || fieldAr.length != nonEmptyNameSize) {
            anonymous = true;
        }
        // init size of table
        numOfFields = typeSize;
        // init item of table
        items = new TDItem[numOfFields];
        for (int i = 0; i < typeSize; i++) {
            // fieldAr
            items[i] = new TDItem(typeAr[i], anonymous ? UN_NAMED_FIELD + i : fieldAr[i]);
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new Iterator<TDItem>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < items.length;
            }

            @Override
            public TDItem next() {
                if (index >= items.length) {
                    throw new NoSuchElementException("no such item");
                }
                return items[index++];
            }
        };
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return numOfFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        checkIndexForItem(i);
        return anonymous ? UN_NAMED_FIELD + i : items[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        checkIndexForItem(i);
        return items[i].fieldType;
    }

    private void checkIndexForItem(int i) {
        if (i >= numOfFields || i < 0) {
            throw new NoSuchElementException("no such index of field");
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        // check name is null
        for (int i = 0; i < items.length; i++) {
            if (items[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("no such element for name: " + name);
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        // TODO: confused until now
        // it seems like calculate total size of all fieldType length
        int size = 0;
        for (TDItem item : items) {
            size += item.fieldType.getLen();
        }
        return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        TupleDesc another = (TupleDesc) o;
        if (another.numOfFields != this.numOfFields || another.anonymous != this.anonymous) {
            return false;
        }
        for (int i = 0; i < this.items.length; i++) {
            TDItem thisItem = this.items[i];
            TDItem anotherItem = another.items[i];
            if (!thisItem.fieldType.equals(anotherItem.fieldType) ||
                    this.anonymous && !thisItem.fieldName.equals(anotherItem.fieldName)) {
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
        // some code goes here
        return Stream.of(items).map(TDItem::toString).collect(Collectors.joining(","));
    }

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;

        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
}
