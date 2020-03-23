package simpledb;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId heapPageId;
    final TupleDesc tupleDesc;
    final byte[] header;
    final Tuple[] tuples;
    final int numSlots;
    boolean dirty;
    TransactionId transactionId;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    private static final int[] bitsMap = new int[256];

    static {
        for (int i = 1; i < 256; ++i) {
            bitsMap[i] = bitsMap[i >> 1] + (i & 1); // x / 2 is x >> 1 and x % 2 is x & 1
        }
    }

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(final HeapPageId id, final byte[] data) throws IOException {
        this.heapPageId = id;
        this.tupleDesc = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++) {
            header[i] = dis.readByte();
        }

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++) {
                tuples[i] = readNextTuple(dis, i);
            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        return BufferPool.getPageSize() * 8 / (tupleDesc.getSize() * 8 + 1);
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        return (int) Math.ceil(getNumTuples() / 8.0);
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            final byte[] oldDataRef;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(heapPageId, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return heapPageId;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(final DataInputStream dis, final int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < tupleDesc.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        final Tuple t = new Tuple(tupleDesc);
        final RecordId rid = new RecordId(heapPageId, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < tupleDesc.numFields(); j++) {
                final Field f = tupleDesc.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        final int len = BufferPool.getPageSize();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        final DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (final byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < tupleDesc.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < tupleDesc.numFields(); j++) {
                final Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        final int zerolen = BufferPool.getPageSize() - (header.length + tupleDesc.getSize() * tuples.length); //- numSlots * td.getSize();
        final byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        final int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(final Tuple t) throws DbException {
        final int tupleNumber = t.getRecordId().getTupleNumber();
        if (!isSlotUsed(tupleNumber)) {
            throw new DbException("The slot is already empty!");
        }
        if (!tuples[tupleNumber].equals(t)) {
            throw new DbException("The tuple is not on the page!");
        }
        markSlotUsed(tupleNumber, false);
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(final Tuple t) throws DbException {
        if (!t.getTupleDesc().equals(tupleDesc)) {
            throw new DbException("Tuple format does not match the page!");
        }
        if (getNumEmptySlots() == 0) {
            throw new DbException("No empty slots on the page!");
        }
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                t.setRecordId(new RecordId(heapPageId, i));
                tuples[i] = t;
                markSlotUsed(i, true);
                break;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(final boolean dirty, final TransactionId tid) {
        this.dirty = dirty;
        transactionId = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return dirty ? transactionId : null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int count = 0;
        for (final Byte head : header) {
            count += bitsMap[head & 0xFF];
        }
        return numSlots - count;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(final int i) {
        return (header[i / 8] >> (i % 8) & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(final int i, final boolean value) {
        if (value) {
            header[i / 8] |= (1 << (i % 8));
        } else {
            header[i / 8] &= ~(1 << (i % 8));
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        return new Iterator<Tuple>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                while (index < numSlots && !isSlotUsed(index)) {
                    index += 1;
                }
                return index < numSlots;
            }

            @Override
            public Tuple next() {
                if (hasNext()) {
                    index += 1;
                    return tuples[index - 1];
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

}

