package simpledb;

import java.io.*;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {

    private final Map<String, DbFile> nameFileMap = new HashMap<>();
    private final Map<Integer, String> idNameMap = new HashMap<>();
    private final Map<Integer, String> idPrimaryKeyMap = new HashMap<>();

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identifier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(final DbFile file, final String name, final String pkeyField) {
        final int id = file.getId();
        nameFileMap.put(name, file);
        idNameMap.put(id, name);
        idPrimaryKeyMap.put(id, pkeyField);
    }

    public void addTable(final DbFile file, final String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(final DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(final String name) throws NoSuchElementException {
        if (!nameFileMap.containsKey(name)) {
            throw new NoSuchElementException();
        }
        return nameFileMap.get(name).getId();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(final int tableid) throws NoSuchElementException {
        if (!idNameMap.containsKey(tableid)) {
            throw new NoSuchElementException();
        }
        return nameFileMap.get(idNameMap.get(tableid)).getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(final int tableid) throws NoSuchElementException {
        if (!idNameMap.containsKey(tableid)) {
            throw new NoSuchElementException();
        }
        return nameFileMap.get(idNameMap.get(tableid));
    }

    public String getPrimaryKey(final int tableid) {
        if (!idPrimaryKeyMap.containsKey(tableid)) {
            throw new NoSuchElementException();
        }
        return idPrimaryKeyMap.get(tableid);
    }

    public Iterator<Integer> tableIdIterator() {
        return idNameMap.keySet().iterator();
    }

    public String getTableName(final int id) {
        if (!idNameMap.containsKey(id)) {
            throw new NoSuchElementException();
        }
        return idNameMap.get(id);
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        nameFileMap.clear();
        idNameMap.clear();
        idPrimaryKeyMap.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile File name
     */
    public void loadSchema(final String catalogFile) {
        String line = "";
        final String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            final BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                final String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                final String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                final String[] els = fields.split(",");
                final ArrayList<String> names = new ArrayList<>();
                final ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    final String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int")) {
                        types.add(Type.INT_TYPE);
                    } else if (els2[1].trim().toLowerCase().equals("string")) {
                        types.add(Type.STRING_TYPE);
                    } else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk")) {
                            primaryKey = els2[0].trim();
                        } else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                final Type[] typeAr = types.toArray(new Type[0]);
                final String[] namesAr = names.toArray(new String[0]);
                final TupleDesc t = new TupleDesc(typeAr, namesAr);
                final HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (final IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (final IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

