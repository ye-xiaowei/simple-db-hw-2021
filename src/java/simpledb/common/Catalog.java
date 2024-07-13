package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    private final Map<Integer, TableDesc> tablesByTableId = new HashMap<>();
    private final Map<String, TableDesc> tablesByTableName = new HashMap<>();

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = readWriteLock.readLock();
    private final Lock writeLock = readWriteLock.writeLock();

    private static class TableDesc {
        private DbFile file;
        private String name;
        private String pkeyField;

        public TableDesc(DbFile file,
                         String name,
                         String pkeyField) {
            this.file = file;
            this.name = name;
            this.pkeyField = pkeyField;
        }

        public void setFile(DbFile file) {
            this.file = file;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setPkeyField(String pkeyField) {
            this.pkeyField = pkeyField;
        }
    }

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        writeLock.lock();
        try {
            TableDesc desc1 = tablesByTableId.get(file.getId());
            TableDesc desc2 = tablesByTableName.get(name);
            if (desc1 != desc2) {
                if (desc1 != null) {
                    tablesByTableId.remove(desc1.file.getId());
                    tablesByTableName.remove(desc1.name);
                }
                if (desc2 != null) {
                    tablesByTableId.remove(desc2.file.getId());
                    tablesByTableName.remove(desc2.name);
                }
            }
            TableDesc desc = new TableDesc(file, name, pkeyField);
            tablesByTableId.put(file.getId(), desc);
            tablesByTableName.put(name, desc);
        } finally {
            writeLock.unlock();
        }
    }

    public void addTable(DbFile file, String name) {
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
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        readLock.lock();
        try {
            TableDesc desc = tablesByTableName.get(name);
            if (desc == null) {
                throw new NoSuchElementException();
            }
            return desc.file.getId();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
        readLock.lock();
        try {
            TableDesc desc = tablesByTableId.get(tableid);
            if (desc == null) {
                throw new NoSuchElementException();
            }
            return desc.file.getTupleDesc();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // some code goes here
        readLock.lock();
        try {
            TableDesc desc = tablesByTableId.get(tableid);
            if (desc == null) {
                throw new NoSuchElementException();
            }
            return desc.file;
        } finally {
            readLock.unlock();
        }
    }

    public String getPrimaryKey(int tableid) {
        // some code goes here
        readLock.lock();
        try {
            TableDesc desc = tablesByTableId.get(tableid);
            if (desc == null) {
                throw new NoSuchElementException();
            }
            return desc.pkeyField;
        } finally {
            readLock.unlock();
        }
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        writeLock.lock();
        try {
            Set<Integer> keySet = tablesByTableId.keySet();
            return keySet.iterator();
        } finally {
            writeLock.unlock();
        }
    }

    public String getTableName(int id) {
        // some code goes here
        readLock.lock();
        try {
            TableDesc desc = tablesByTableId.get(id);
            if (desc == null) {
                throw new NoSuchElementException();
            }
            return desc.name;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        // some code goes here
        writeLock.lock();
        try {
            tablesByTableId.clear();
            tablesByTableName.clear();
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<>();
                ArrayList<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

