/**
 * Copyright (C) 2009 - 2014 Envidatec GmbH <info@envidatec.com>
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
 *
 * JEAPI-SQL is part of the OpenJEVis project, further project information are
 * published at <http://www.OpenJEVis.org/>.
 */
package org.jevis.api.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisConstants;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisExceptionCodes;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisRelationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Florian Simon<florian.simon@envidatec.com>
 */
public class ObjectTable {

    public final static String TABLE = "object";
    public final static String COLUMN_ID = "id";
    public final static String COLUMN_NAME = "name";
    public final static String COLUMN_CLASS = "type";
//    public final static String COLUMN_PARENT = "parent";
    public final static String COLUMN_LINK = "link";
    public final static String COLUMN_DELETE = "deletets";
    public final static String COLUMN_GROUP = "groupid";//remove ID from name
    private Connection _connection;
    private JEVisDataSourceSQL _ds;
    Logger logger = LoggerFactory.getLogger(ObjectTable.class);
    private Map<Long, JEVisObject> _cach = new HashMap<Long, JEVisObject>();

    public ObjectTable(JEVisDataSourceSQL ds) throws JEVisException {
        _connection = ds.getConnection();
        _ds = ds;
    }

    public JEVisObject loginUser(String name, String pw) throws JEVisException {
        String sql = "select " + TABLE + ".*"
                + "," + SampleTable.TABLE + "." + SampleTable.COLUMN_VALUE
                + " from " + TABLE
                + " left join " + SampleTable.TABLE
                + " on " + TABLE + "." + COLUMN_ID + "=" + SampleTable.TABLE + "." + SampleTable.COLUMN_OBJECT
                + " where " + TABLE + "." + COLUMN_NAME + "=?"
                + " and " + TABLE + "." + COLUMN_CLASS + "=?"
                + " and " + SampleTable.TABLE + "." + SampleTable.COLUMN_ATTRIBUTE + "=?"
                + " ORDER BY " + SampleTable.TABLE + "." + SampleTable.COLUMN_TIMESTAMP + " DESC"
                + " limit 1";

        PreparedStatement ps = null;
        ResultSet rs = null;
        JEVisObject object = null;
        _ds.addQuery();

        try {

            ps = _connection.prepareStatement(sql);
            ps.setString(1, name);
            ps.setString(2, JEVisConstants.Class.USER);
            ps.setString(3, JEVisConstants.Attribute.USER_PASSWORD);

            logger.debug("SQL: {}", ps.toString());

            rs = ps.executeQuery();

            while (rs.next()) {
                String dbpw = rs.getString(SampleTable.COLUMN_VALUE);
                if (PasswordHash.validatePassword(pw, dbpw)) {
//                    object = new JEVisObjectSQL(_ds, rs);
                    object = getObject(rs.getLong(COLUMN_ID), false);
                } else {
                    throw new JEVisException("User does not exist or password was wrong", JEVisExceptionCodes.UNAUTHORIZED);
                }

            }

            return object;
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
    }

    public List<JEVisObject> getObjects(List<JEVisClass> classes) throws JEVisException {
        String sql = "select " + TABLE + ".*"
                + "," + RelationshipTable.TABLE + ".*"
                + " from " + TABLE
                + " left join " + RelationshipTable.TABLE
                + " on " + TABLE + "." + COLUMN_ID + "=" + RelationshipTable.TABLE + "." + RelationshipTable.COLUMN_START
                + " or " + TABLE + "." + COLUMN_ID + "=" + RelationshipTable.TABLE + "." + RelationshipTable.COLUMN_END
                + " where " + TABLE + "." + COLUMN_CLASS + " in (";

        PreparedStatement ps = null;
        ResultSet rs = null;
        List<JEVisObject> objects = new ArrayList<JEVisObject>();
        _ds.addQuery();

        try {
            boolean firstRun = true;
            for (JEVisClass cl : classes) {
                if (firstRun) {
                    sql += "?";
                    firstRun = false;
                } else {
                    sql += ",?";
                }
            }
            sql += ") and " + TABLE + "." + COLUMN_DELETE + " is null";

            ps = _connection.prepareStatement(sql);

            for (int i = 0; i < classes.size(); i++) {
                JEVisClass cl = classes.get(i);
                ps.setString(i + 1, cl.getName());//index start with 1 for prepareStatements...
            }

            rs = ps.executeQuery();
            logger.error("getObject.SQl : {}. ", ps);

            while (rs.next()) {

                //TODO:replace, this is an not so opimal way to load all relationships for an Object in one sql query
                boolean isCache = false;
                for (JEVisObject ob : objects) {
                    if (ob.getID().equals(rs.getLong(COLUMN_ID))) {
                        isCache = true;
                        ((JEVisObjectSQL) ob).addRelationship(new JEVisRelationshipSQL(_ds, rs));
                    }
                }

                if (!isCache) {
                    JEVisObjectSQL newObj = new JEVisObjectSQL(_ds, rs);
                    newObj.addRelationship(new JEVisRelationshipSQL(_ds, rs));
                    objects.add(newObj);
                }

            }

            return objects;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("Error while inserting object", 234234, ex);//ToDo real number
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logger.debug("Error while closing DB connection: {}. ", ex);
                }
            }
        }
    }

    public JEVisObject insertObject(String name, JEVisClass jclass, JEVisObject parent, long group) throws JEVisException {
        String sql = "insert into " + TABLE
                + "(" + COLUMN_NAME + "," + COLUMN_CLASS + " )"
                + " values(?,?)";
        PreparedStatement ps = null;
        _ds.addQuery();

        try {
            ps = _connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setString(2, jclass.getName());
//            System.out.println("putObject: " + ps);

            int count = ps.executeUpdate();
            if (count == 1) {
                ResultSet rs = ps.getGeneratedKeys();

                if (rs.next()) {
                    //add ownership
                    for (JEVisRelationship rel : parent.getRelationships()) {
                        if (rel.isType(JEVisConstants.ObjectRelationship.OWNER) && rel.getStartObject().getID() == parent.getID()) {
                            _ds.getRelationshipTable().insert(rs.getLong(1), rel.getEndObject().getID(), JEVisConstants.ObjectRelationship.OWNER);
                        }
                    }

                    //and parenshiop or NESTEDT_CLASS depending on the Class
                    int relType = JEVisConstants.ObjectRelationship.PARENT;//not very save
//                    if (RelationsManagment.isParentRelationship(parent.getJEVisClass(), jclass)) {
//                        relType = JEVisConstants.ObjectRelationship.PARENT;
//                    } else 

                    if (RelationsManagment.isNestedRelationship(parent.getJEVisClass(), jclass)) {
                        relType = JEVisConstants.ObjectRelationship.NESTED_CLASS;
                    }
                    _ds.getRelationshipTable().insert(rs.getLong(1), parent.getID(), relType);

                    return getObject(rs.getLong(1), false);
                } else {
                    throw new JEVisException("Error selecting insertedt object", 234235);//ToDo real number
                }
            } else {
                throw new JEVisException("Error while inserting object", 234236);//ToDo real number
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("Error while inserting object", 234234, ex);//ToDo real number
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

    public JEVisObject updateObject(JEVisObject object) throws JEVisException {
        String sql = "update " + TABLE
                + " set " + COLUMN_NAME + "=?"
                + " where " + COLUMN_ID + "=?";

        PreparedStatement ps = null;
        ResultSet rs = null;
        _ds.addQuery();

        try {
            ps = _connection.prepareStatement(sql);
            ps.setString(1, object.getName());
            ps.setLong(2, object.getID());

            int count = ps.executeUpdate();
            if (count == 1) {
                return object;//TODO: maybe reselect the object but since we only change the name pff
            } else {
                throw new JEVisException("Error while updating object", 234236);//ToDo real number
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("Error while updating object", 234234, ex);//ToDo real number
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logger.debug("Error while closing DB connection: {}. ", ex);
                }
            }
        }

    }

    public JEVisObject insertLink(String name, JEVisObject linkParent, JEVisObject linkedObject) throws JEVisException {
        String sql = "insert into " + TABLE
                + "(" + COLUMN_NAME + "," + COLUMN_GROUP + "," + COLUMN_LINK + "," + COLUMN_CLASS + ")"
                + " values(?,?,?,?)";
        JEVisObject newObject = null;
        _ds.addQuery();

        try {
            PreparedStatement ps = _connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
//            PreparedStatement ps = _connection.prepareStatement(sql);
            ps.setString(1, name);
            ps.setLong(2, 0);//TODO replace this is not needet anymore
            ps.setLong(3, linkedObject.getID());
            ps.setString(4, "Link");

//            System.out.println("putObjectLink.sql: " + ps);
            int count = ps.executeUpdate();
            if (count == 0) {
                System.out.println("Create faild");
            }

            ResultSet rs = ps.getGeneratedKeys();
//             Statement.RETURN_GENERATED_KEYS);
            if (rs.next()) {
                newObject = getObject(rs.getLong(1), false);
            }

            ps.close();//not save
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("Error while creating object link", 234234, ex);//ToDo real number
        }

        return newObject;
    }

    public JEVisObject getObject(Long id, boolean cach) throws JEVisException {
        logger.debug("getObject: {} ", id);

        if (cach) {
//            logger.debug("using cach is on");
            if (_cach.containsKey(id)) {
                logger.debug("found object in cach");
                return _cach.get(id);
            }
            logger.debug("Object not in cach");
        }

        JEVisObjectSQL object = null;
        _ds.addQuery();

        String sql = "select o.*"
                + ",r.*"
                + " from " + TABLE + " o"
                + " left join " + RelationshipTable.TABLE + " r"
                + " on o." + COLUMN_ID + "=" + "r." + RelationshipTable.COLUMN_START
                + " or o." + COLUMN_ID + "=" + "r." + RelationshipTable.COLUMN_END
                + " where  o." + COLUMN_ID + "=?"
                + " and o." + COLUMN_DELETE + " is null";
//                + " limit 1 ";

        PreparedStatement ps = null;

        try {
            ps = _connection.prepareStatement(sql);
            ps.setLong(1, id);

            logger.debug("getObject.sql: {} ", ps);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                if (object == null) {
                    object = new JEVisObjectSQL(_ds, rs);
                }
                object.addRelationship(new JEVisRelationshipSQL(_ds, rs));
            }

        } catch (SQLException ex) {
            logger.error("Error while selecting Object: {} ", ex.getMessage());
            throw new JEVisException("Error while selecting Object", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.error("Error while selecting Object: {} ", ex);
            throw new JEVisException("Error while selecting Object", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }

        _cach.put(id, object);
        return object;
    }

//    public List<JEVisObject> getChildren(JEVisObject obj) throws JEVisException {
//        ResultSet rs = null;
//        List<JEVisObject> children = new ArrayList<JEVisObject>();
//        _ds.addQuery();
//
//        String sql = "select o.*"
//                + ",r.*"
//                + " from " + TABLE + " o"
//                + " left join " + RelationshipTable.TABLE + " r"
//                + " on o." + COLUMN_ID + "=" + "r." + RelationshipTable.COLUMN_START
//                + " or o." + COLUMN_ID + "=" + "r." + RelationshipTable.COLUMN_END
//                + " where  o." + COLUMN_PARENT + "=?"
//                + " and o." + COLUMN_DELETE + " is null";
//
//        try {
//            PreparedStatement ps = _connection.prepareStatement(sql);
//            ps.setLong(1, obj.getID());
//
//
////        System.out.println("getObject.sql: "+ps);
//            rs = ps.executeQuery();
//
//            Map<Long, JEVisObjectSQL> _tmp = new HashMap<Long, JEVisObjectSQL>();
//
//            while (rs.next()) {
//                long id = rs.getLong(COLUMN_ID);
//                if (!_tmp.containsKey(id)) {
//                    _tmp.put(id, new JEVisObjectSQL(_ds, rs));
//                }
//                _tmp.get(id).addRelationship(new JEVisRelationshipSQL(_ds, rs));
//
//            }
//            children = new LinkedList<JEVisObject>(_tmp.values());
//
//        } catch (SQLException ex) {
//            throw new JEVisException("Error while select Child objects", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
//        } finally {
//            if (rs != null) {
//                try {
//                    rs.close();
//                } catch (SQLException ex) {
//                    logger.debug("Error while closing DB connection: {}. ", ex);
//                }
//            }
//
//        }
//
//
//        return children;
//    }
    private void findChildren(List<Long> children, Map<Long, Long> allID, Long parent) {
        for (Map.Entry<Long, Long> entry : allID.entrySet()) {
//            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
            if (entry.getValue() == parent) {
//                System.out.println("found child");
                children.add(entry.getKey());
                findChildren(children, allID, entry.getKey());
            }

        }

    }

    //TODO make a fast version , myabe travel the tree...hmm mybe not
//    private List<Long> getAllChildren(long id) throws JEVisException {
//        String sql = "select " + COLUMN_ID + "," + COLUMN_PARENT + " from " + TABLE;
//
//        PreparedStatement ps = null;
//        ResultSet rs = null;
//        Map<Long, Long> allID = new HashMap<Long, Long>();
//        List<Long> children = new ArrayList<Long>();
//
//        try {
//            ps = _connection.prepareStatement(sql);
//
//            rs = ps.executeQuery();
//
//
//            while (rs.next()) {
//
//                //------------ Parent gibt es nicht mehr -----------------
////                System.out.println("add to map: ID:"+rs.getLong(COLUMN_ID)+"  Parent:"+rs.getLong(COLUMN_PARENT));
//
//                allID.put(rs.getLong(COLUMN_ID), rs.getLong(COLUMN_PARENT));
//
//            }
//
//            findChildren(children, allID, id);
//            return children;
//
//
//        } catch (Exception ex) {
//            ex.printStackTrace();
//            //TODO error handling
//            return children;
//        } finally {
//            if (rs != null) {
//                try {
//                    rs.close();
//                } catch (SQLException e) { /*ignored*/ }
//            }
//            if (ps != null) {
//                try {
//                    ps.close();
//                } catch (SQLException e) { /*ignored*/ }
//            }
//        }
//    }
    public List<JEVisObject> getAllChildren(List<JEVisObject> objs, JEVisObject obj) {
        try {
            for (JEVisObject ch : obj.getChildren()) {
                objs.add(ch);
                getAllChildren(objs, ch);
            }
        } catch (Exception ex) {
        }
        return objs;
    }

    public boolean deleteObject(JEVisObject obj) throws JEVisException {
        String sql = "update " + TABLE
                + " set " + COLUMN_DELETE + "=?"
                + " where " + COLUMN_ID + " IN(?";
        PreparedStatement ps = null;
        _ds.addQuery();

        try {

            List<JEVisObject> children = getAllChildren(new ArrayList<JEVisObject>(), obj);
            children.add(obj);

            for (JEVisObject ch : children) {
                sql += ",?";
            }
            sql += ")";

            Calendar now = new GregorianCalendar();

            ps = _connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setTimestamp(1, new Timestamp(now.getTimeInMillis()));
            ps.setLong(2, obj.getID());

            for (int i = 0; i < children.size(); i++) {
                JEVisObject ch = children.get(i);
                ps.setLong(i + 3, children.get(i).getID());
            }

            int count = ps.executeUpdate();

            if (count > 0) {
                List<Long> ids = new LinkedList<Long>();
                for (JEVisObject ch : children) {
                    _cach.remove(ch.getID());
                    ids.add(ch.getID());
                }

                _ds.getRelationshipTable().deleteAll(ids);

                return true;
            } else {
                return false;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            //TODO throw JEVisExeption?!
            return false;
        } finally {
//            if (rs != null) {
//                try {
//                    rs.close();
//                } catch (SQLException e) { /*ignored*/ }
//            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }
    }

    public boolean isUserUnique(String name) {
        String sql = "select " + COLUMN_ID + " from " + TABLE
                + " where " + COLUMN_NAME + "=?";
        PreparedStatement ps = null;
        _ds.addQuery();

        try {

            ps = _connection.prepareStatement(sql);
            ps.setString(1, name);

            ResultSet rs = ps.executeQuery();

            if (rs.next()) {
                return false;
            } else {
                return true;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            //TODO throw JEVisExeption?!
            return false;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }
    }
}
