/**
 * Copyright (C) 2009 - 2013 Envidatec GmbH <info@envidatec.com>
 *
 * This file is part of JEWebService.
 *
 * JEWebService is free software: you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation in version 3.
 *
 * JEWebService is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * JEWebService. If not, see <http://www.gnu.org/licenses/>.
 *
 * JEWebService is part of the OpenJEVis project, further project information
 * are published at <http://www.OpenJEVis.org/>.
 */
package org.jevis.jeapi.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisExceptionCodes;
import org.slf4j.Logger;
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
    private Map<String, JEVisClass> _cach = new HashMap<String, JEVisClass>();
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

    public boolean delete(JEVisClass jlas) throws JEVisException {
        String sql = "delete from " + TABLE + " where " + COLUMN_NAME + "=?";
        PreparedStatement ps = null;
        ResultSet rs = null;
        _ds.addQuery();

        try {
            ps = _connection.prepareStatement(sql);
            ps.setString(1, jlas.getName());

            int count = ps.executeUpdate();

            if (count == 1) {
                _cach.remove(jlas.getName());
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
        String sql = "select * from " + TABLE;
        PreparedStatement ps = null;
        List<JEVisClass> all = new ArrayList<JEVisClass>();
        List<JEVisClass> heirs = new ArrayList<JEVisClass>();
        _ds.addQuery();

        try {
            ps = _connection.prepareStatement(sql);
//            System.out.println("getAllClasses: " + ps);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                all.add(new JEVisClassSQL(_ds, rs));
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
        _ds.addQuery();

        try {

            ps = _connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
//        ps.setString(2, discription); 

//        System.out.println("putClass.sql: " + ps);
//        int value = ps.executeUpdate(sql, Statement.RETURN_GENERATED_KEYS);
            int value = ps.executeUpdate();
            if (value == 1) {
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

        try {
            String sql = "update " + TABLE
                    + " set " + COLUMN_DESCRIPTION + "=?," + COLUMN_NAME + "=?," + COLUMN_UNIQUE + "=?"
                    + " where " + COLUMN_NAME + "=?";

            _ds.addQuery();
            PreparedStatement ps = _connection.prepareStatement(sql);


            if (!oldName.equals(jclass.getName())) { // ->rename
//                System.out.println("rename");
                ps.setString(1, jclass.getDescription());
                ps.setString(2, jclass.getName());
                ps.setBoolean(3, jclass.isUnique());
                ps.setString(4, oldName);
            } else {//update with same name
//                System.out.println("update");
                if (jclass.getDescription() != null) {
                    ps.setString(1, jclass.getDescription());
                } else {
                    ps.setNull(1, Types.VARCHAR);
                }

                ps.setString(2, jclass.getName());
                if (jclass.getInheritance() != null && jclass.getInheritance().getName() != null) {
                    ps.setString(3, jclass.getInheritance().getName());
                } else {
                    ps.setNull(3, Types.VARCHAR);
                }

                //TODO hier warst du, lösche classe vor test (unique=keyword?)
                ps.setBoolean(4, jclass.isUnique());
                ps.setString(5, jclass.getName());
            }

            System.out.println("sql: " + ps);

            int res = ps.executeUpdate();

            //Check if the name changed, if yes we have to change all existing JEVisObjects.....do we want that?
            if (res == 1) {
                if (!oldName.equals(jclass.getName())) {
                    //------------------ Update exintings objects
                    System.out.println("update Existing JEVis Objects");
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
                    System.out.println("update Existing valid JEVis parents");
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

    public JEVisClass getObjectClass(String name, boolean cach) throws JEVisException {
        JEVisClass jClass = null;

        //TODO:reenable cach disable cach
//        if (cach) {
//            if (_cach.containsKey(name)) {
//                return _cach.get(name);
//            }
//        }

        String sql = "select * from " + TABLE
                + " where  " + COLUMN_NAME + "=?"
                + " limit 1 ";

        PreparedStatement ps = null;
        _ds.addQuery();


        try {
            ps = _connection.prepareStatement(sql);
            ps.setString(1, name);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                jClass = new JEVisClassSQL(_ds, rs);
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

        _cach.put(name, jClass);
        return jClass;
    }

    public List<JEVisClass> getAllObjectClasses() throws JEVisException, SQLException {
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
        _ds.addQuery();


        try {
            ps = _connection.prepareStatement(sql);

            rs = ps.executeQuery();

            while (rs.next()) {
                jClasses.add(new JEVisClassSQL(_ds, rs));
            }

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
