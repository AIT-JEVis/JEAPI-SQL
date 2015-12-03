/**
 * Copyright (C) 2013 - 2015 Envidatec GmbH <info@envidatec.com>
 *
 * This file is part of JEAPI-SQL.
 *
 * JEAPI-SQL is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation in version 3.
 *
 * JEAPI-SQL is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * JEAPI-SQL. If not, see <http://www.gnu.org/licenses/>.
 */
package org.jevis.api.sql;

import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisClassRelationship;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisExceptionCodes;
import org.jevis.api.JEVisType;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Florian Simon<florian.simon@envidatec.com>
 */
public class ClassTable {

    public final static String TABLE = "objectclass";
    public final static String COLUMN_NAME = "name";
//    public final static String COLUMN_INHERIT = "inheritance";
    public final static String COLUMN_DESCRIPTION = "description";
    public final static String COLUMN_ICON = "icon";//-->editinal table for files ?
    public final static String COLUMN_UNIQUE = "isunique";
    private Connection _connection;
    private JEVisDataSourceSQL _ds;
//    private final SimpleClassCache _cache = SimpleClassCache.getInstance();
//    private Map<String, JEVisClass> _cach = new HashMap<String, JEVisClass>();
    org.slf4j.Logger logger = LoggerFactory.getLogger(ClassTable.class);

    public ClassTable(JEVisDataSourceSQL ds) throws JEVisException {
        _connection = ds.getConnection();
        _ds = ds;
    }

    private void findHeirs(List<JEVisClass> all, List<JEVisClass> heirs, JEVisClass jclass) throws JEVisException {
        for (JEVisClass heir : all) {
            if (heir.getInheritance().equals(jclass)) {
                heirs.add(heir);
                findHeirs(all, heirs, heir);
            }
        }
    }

    public boolean delete(JEVisClass jclass) throws JEVisException {
        String sql = "delete from " + TABLE + " where " + COLUMN_NAME + "=?";
        PreparedStatement ps = null;
        ResultSet rs = null;
        _ds.addQuery("ClassTable.delete");

        try {
            for (JEVisType type : jclass.getTypes()) {
                if (jclass.getInheritance() != null) {
                    if (jclass.getInheritance().getType(type.getName()) == null) {
                        _ds.getTypeTable().detele(type);
                    }
                } else {
                    _ds.getTypeTable().detele(type);
                }

            }

            for (JEVisClassRelationship rel : jclass.getRelationships()) {
                _ds.getClassRelationshipTable().delete(rel);
            }

            ps = _connection.prepareStatement(sql);
            ps.setString(1, jclass.getName());

            System.out.println("delete.class: " + ps.toString());
            int count = ps.executeUpdate();

            if (count == 1) {
                SimpleClassCache.getInstance().remove(jclass.getName());

                return true;
            } else {
                return false;
            }

        } catch (Exception ex) {
            throw new JEVisException("Error while deleting Class: " + ex, 2342763);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex) {
                    logger.warn("cound not close Prepared statement: {}", ex);
                }
            }
        }
    }

    public List<JEVisClass> getAllHeirs(JEVisClass jlass) throws JEVisException {
        System.out.println("getAllHeirs()");
        String sql = "select * from " + TABLE;
        PreparedStatement ps = null;
        List<JEVisClass> all = new ArrayList<JEVisClass>();
        List<JEVisClass> heirs = new ArrayList<JEVisClass>();
        _ds.addQuery("ClassTable.getAllHeirs");

        try {
            ps = _connection.prepareStatement(sql);
//            System.out.println("getAllClasses: " + ps);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String nName = rs.getString(ClassTable.COLUMN_NAME);
                if (!SimpleClassCache.getInstance().contains(nName)) {
                    SimpleClassCache.getInstance().addClass(new JEVisClassSQL(_ds, rs));
                }
                all.add(SimpleClassCache.getInstance().getJEVisClass(nName));

            }

            findHeirs(all, heirs, jlass);

            return heirs;

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("Error while searching Class heirs", 234234, ex);//ToDo real number
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex) {
                    logger.warn("cound not close Prepared statement: {}", ex);
                }
            }
        }
    }

    //TODO: this
    public boolean insert(String name) throws JEVisException {
        String sql = "insert into " + TABLE
                + "(" + COLUMN_NAME + ")"
                + " values(?)";

        PreparedStatement ps = null;
        _ds.addQuery("ClassTable.insert");

        try {

            ps = _connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);

//        ps.setString(2, discription);
//            System.out.println("putClass.sql: " + ps);
//        int value = ps.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            int value = ps.executeUpdate();
            if (value == 1) {
                getObjectClass(name);
                return true;
            } else {
                return false;
            }

        } catch (Exception ex) {

            ex.printStackTrace();
            throw new JEVisException("User does not exist or password was wrong", JEVisExceptionCodes.UNAUTHORIZED);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex) {
                    logger.debug("Error while closing DB connection: {}. ", ex);
                }
            }
        }
    }

    public boolean update(JEVisClass jclass, String oldName) throws JEVisException {

//        System.out.println("#######");
//        System.out.println("jclass.name: " + jclass);
//
//        System.out.println("ClassTable.update+ " + ((JEVisClassSQL) jclass).getFile());
        try {
            String sql = "update " + TABLE
                    + " set " + COLUMN_DESCRIPTION + "=?," + COLUMN_NAME + "=?," + COLUMN_UNIQUE + "=?";// + COLUMN_ICON + "=?"

            if (((JEVisClassSQL) jclass).getFile() != null || jclass.getIcon() != null) {
                sql += ", " + COLUMN_ICON + "=?";
            }
            sql += " where " + COLUMN_NAME + "=?";

            _ds.addQuery("ClassTable.update");
            PreparedStatement ps = _connection.prepareStatement(sql);

            int i = 1;

//            if (!oldName.equals(jclass.getName())) { // ->rename
//                System.out.println("rename");
            ps.setString(i++, jclass.getDescription());
            ps.setString(i++, jclass.getName());
            ps.setBoolean(i++, jclass.isUnique());

            if (((JEVisClassSQL) jclass).getFile() != null) {
                File file = ((JEVisClassSQL) jclass).getFile();
                FileInputStream fis = new FileInputStream(file);
                ps.setBinaryStream(i++, fis, (int) file.length());
            } else if (jclass.getIcon() != null) {
//                System.out.println("usinf icon");
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                ImageIO.write(jclass.getIcon(), "gif", os);
                InputStream is = new ByteArrayInputStream(os.toByteArray());

                ps.setBinaryStream(i++, is);
            }

            ps.setString(i++, oldName);

//            } else {//update with same name
////                System.out.println("update");
//                if (jclass.getDescription() != null) {
//                    ps.setString(1, jclass.getDescription());
//                } else {
//                    ps.setNull(1, Types.VARCHAR);
//                }
//
//                ps.setString(2, jclass.getName());
//
////                if (jclass.getInheritance() != null && jclass.getInheritance().getName() != null) {
////                    ps.setString(3, jclass.getInheritance().getName());
////                } else {
////                    ps.setNull(3, Types.VARCHAR);
////                }
//
//                //TODO hier warst du, lösche classe vor test (unique=keyword?)
//                ps.setBoolean(3, jclass.isUnique());
//
//
//
//                ps.setString(4, jclass.getName());
//            }
//            System.out.println("update class: " + ps);
            int res = ps.executeUpdate();

            //Check if the name changed, if yes we have to change all existing JEVisObjects.....do we want that?
            if (res == 1) {
                if (!oldName.equals(jclass.getName())) {
                    //------------------ Update exintings objects
//                    System.out.println("update Existing JEVis Objects");
                    String updateObject = "update " + ObjectTable.TABLE
                            + " set " + ObjectTable.COLUMN_CLASS + "=?"
                            + " where " + ObjectTable.COLUMN_CLASS + "=?";
                    PreparedStatement ps2 = _connection.prepareStatement(updateObject);
                    ps2.setString(1, jclass.getName());
                    ps2.setString(2, oldName);

                    int res2 = ps2.executeUpdate();
                    if (res2 > 0) {
//                        System.out.println("updated " + res2 + " JEVisObjects");
                    }

                    //------------------- Update existing Valid Parents
//                    System.out.println("update Existing valid JEVis parents");
                    String updateValid = "update " + ClassRelationTable.TABLE
                            + " set " + ClassRelationTable.COLUMN_START + "=?"
                            + " where " + ClassRelationTable.COLUMN_START + "=?";
                    PreparedStatement psUpdate = _connection.prepareStatement(updateValid);
                    psUpdate.setString(1, jclass.getName());
                    psUpdate.setString(2, oldName);

                    psUpdate.executeUpdate();

                    //------------------- Update existing Valid Parents #2
//                    System.out.println("update Existing valid JEVis parents");
                    String updateValid2 = "update " + ClassRelationTable.TABLE
                            + " set " + ClassRelationTable.COLUMN_END + "=?"
                            + " where " + ClassRelationTable.COLUMN_END + "=?";
                    PreparedStatement psUpdate2 = _connection.prepareStatement(updateValid2);
                    psUpdate2.setString(1, jclass.getName());
                    psUpdate2.setString(2, oldName);

                    psUpdate2.executeUpdate();

                }

                return true;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return false;
    }

    private static java.awt.image.BufferedImage convertToBufferedImage(ImageIcon icon) {
        java.awt.image.BufferedImage bi = new java.awt.image.BufferedImage(
                icon.getIconWidth(),
                icon.getIconHeight(),
                BufferedImage.TYPE_INT_RGB);
        Graphics g = bi.createGraphics();
        // paint the Icon to the BufferedImage.
        icon.paintIcon(null, g, 0, 0);
        g.dispose();
        return bi;

    }

    private byte[] getIconBytes(ImageIcon icon) throws Exception {
        BufferedImage img = convertToBufferedImage(icon);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(1000);
        ImageIO.write(img, "jpg", baos);
        baos.flush();

        return baos.toByteArray();
    }

    public JEVisClass getObjectClass(String name) throws JEVisException {

//        System.out.println("getObjectClass() " + name);
        JEVisClass jClass = null;

        String sql = "select * from " + TABLE
                + " where  " + COLUMN_NAME + "=?"
                + " limit 1 ";

        PreparedStatement ps = null;
        _ds.addQuery("ClassTable.getObjectClass");

        try {
            ps = _connection.prepareStatement(sql);
            ps.setString(1, name);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String nName = rs.getString(ClassTable.COLUMN_NAME);
                JEVisClassSQL newClass = new JEVisClassSQL(_ds, rs);
                SimpleClassCache.getInstance().addClass(newClass);

                jClass = SimpleClassCache.getInstance().getJEVisClass(nName);
            }

        } catch (Exception ex) {

            ex.printStackTrace();
            throw new JEVisException("User does not exist or password was wrong", JEVisExceptionCodes.UNAUTHORIZED);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex) {
                    logger.debug("Error while closing DB connection: {}. ", ex);
                }
            }
        }

        return jClass;
    }

    //TODO reimplementiert
    public JEVisClass getObjectClass(String name, boolean cach) throws JEVisException {
        return SimpleClassCache.getInstance().getJEVisClass(name);
//
//
//        System.out.println("getObjectClass() " + cach);
//        JEVisClass jClass = null;
//
//        //TODO:reenable cach disable cach
////        if (cach) {
////            if (_cach.containsKey(name)) {
////                return _cach.get(name);
////            }
////        }
//        String sql = "select * from " + TABLE
//                + " where  " + COLUMN_NAME + "=?"
//                + " limit 1 ";
//
//        PreparedStatement ps = null;
//        _ds.addQuery();
//
//        try {
//            ps = _connection.prepareStatement(sql);
//            ps.setString(1, name);
//
//            ResultSet rs = ps.executeQuery();
//
//            while (rs.next()) {
//                String nName = rs.getString(ClassTable.COLUMN_NAME);
//                if (!SimpleClassCache.getInstance().contains(nName)) {
//                    SimpleClassCache.getInstance().addClass(new JEVisClassSQL(_ds, rs));
//                }
//
//                jClass = SimpleClassCache.getInstance().getJEVisClass(nName);
//            }
//
//        } catch (Exception ex) {
//
//            ex.printStackTrace();
//            throw new JEVisException("User does not exist or password was wrong", JEVisExceptionCodes.UNAUTHORIZED);
//        } finally {
//            if (ps != null) {
//                try {
//                    ps.close();
//                } catch (SQLException ex) {
//                    logger.debug("Error while closing DB connection: {}. ", ex);
//                }
//            }
//        }
//
//        return jClass;
    }

    public List<JEVisClass> getAllObjectClasses() throws JEVisException, SQLException {
//        System.out.println("getAllObjectClasses()");
        List<JEVisClass> jClasses = new LinkedList<JEVisClass>();

        //TODO:reenable cach disable cach
//        if (cach) {
//            if (_cach.containsKey(name)) {
//                return _cach.get(name);
//            }
//        }
        String sql = "select * from " + TABLE;

        PreparedStatement ps = null;
        ResultSet rs = null;
        _ds.addQuery("ClassTable.getAllObjectClasses");

        try {
            ps = _connection.prepareStatement(sql);

            rs = ps.executeQuery();

            while (rs.next()) {
                String nName = rs.getString(ClassTable.COLUMN_NAME);
                if (!SimpleClassCache.getInstance().contains(nName)) {
                    SimpleClassCache.getInstance().addClass(new JEVisClassSQL(_ds, rs));
                }
                jClasses.add(SimpleClassCache.getInstance().getJEVisClass(nName));
            }

//            System.out.println("getAllObjectClasses()----> end");
        } catch (Exception ex) {

            ex.printStackTrace();
            throw new JEVisException("User does not exist or password was wrong", JEVisExceptionCodes.UNAUTHORIZED);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logger.debug("Error while closing DB connection: {}. ", ex);
                }
            }
        }

        return jClasses;
    }
}
