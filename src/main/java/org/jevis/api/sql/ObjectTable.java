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
import java.util.Arrays;
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
import org.jevis.commons.JEVisUser;
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

    public JEVisObject loginUser(JEVisDataSourceSQL ds, String name, String pw) throws JEVisException {
        /*
         String sql = "select " + TABLE + ".*"
         + "," + SampleTable.TABLE + "." + SampleTable.COLUMN_VALUE
         + " from " + TABLE
         + " left join " + SampleTable.TABLE
         + " on " + TABLE + "." + COLUMN_ID + "=" + SampleTable.TABLE + "." + SampleTable.COLUMN_OBJECT
         + " where " + TABLE + "." + COLUMN_NAME + "=?"
         + " and " + TABLE + "." + COLUMN_CLASS + "=?"
         //                + " and " + SampleTable.TABLE + "." + SampleTable.COLUMN_ATTRIBUTE + "=?"
         + " and ("
         + SampleTable.TABLE + "." + SampleTable.COLUMN_ATTRIBUTE + "=?"
         + " or " + SampleTable.TABLE + "." + SampleTable.COLUMN_ATTRIBUTE + "=?"
         + ")"
         + " ORDER BY " + SampleTable.TABLE + "." + SampleTable.COLUMN_TIMESTAMP + " DESC"
         + " limit 1";
         */

        String sqlUser = "select * from " + TABLE
                + " where " + COLUMN_NAME + "=?"
                + " and " + COLUMN_CLASS + "=?"
                + " and " + COLUMN_DELETE + " is null"
                + " limit 1";

        String sqlAttributes = "select s.*"
                + " from "
                + " ( select " + SampleTable.COLUMN_ATTRIBUTE + ", MAX(" + SampleTable.COLUMN_TIMESTAMP + ") as maxtime"
                + " from " + SampleTable.TABLE + " where " + SampleTable.COLUMN_OBJECT + "=? group by " + SampleTable.COLUMN_ATTRIBUTE
                + " ) as m "
                + " inner join " + SampleTable.TABLE
                + " s ON s." + SampleTable.COLUMN_ATTRIBUTE + "=m." + SampleTable.COLUMN_ATTRIBUTE
                + " AND s." + SampleTable.COLUMN_OBJECT + "=?"
                + " AND m.maxtime=s." + SampleTable.COLUMN_TIMESTAMP;

        PreparedStatement ps = null;
        ResultSet rs = null;
        JEVisObject object = null;
        _ds.addQuery("ObjectTable.loginUser");

        try {
            //First get the use by name, the create User securs the there is only one user per name
            ps = _connection.prepareStatement(sqlUser);
            ps.setString(1, name);
            ps.setString(2, JEVisConstants.Class.USER);

            logger.debug("SQL: {}", ps.toString());

            rs = ps.executeQuery();

            while (rs.next()) {

//                object = new JEVisObjectSQL(_ds, rs);
                object = getObject(rs.getLong(COLUMN_ID), false);
                SimpleObjectCache.getInstance().addObject(object);

            }

            if (object != null) {
                JEVisUser user = new JEVisUser();
                ds.setCurrentUserObject(user);

                ps = _connection.prepareStatement(sqlAttributes);
                ps.setLong(1, object.getID());

                ps.setLong(2, object.getID());
                rs = ps.executeQuery();

                while (rs.next()) {

                    String attribute = rs.getString(SampleTable.COLUMN_ATTRIBUTE);

                    if (attribute.equals(JEVisConstants.Attribute.USER_PASSWORD)) {
                        String dbpw = rs.getString(SampleTable.COLUMN_VALUE);
//                        System.out.println("pw: " + dbpw);
                        if (PasswordHash.validatePassword(pw, dbpw)) {
//                            System.out.println("Password OK");
                        } else {
                            throw new JEVisException("User does not exist or password was wrong", JEVisExceptionCodes.UNAUTHORIZED);
                        }
                    } else if (attribute.equals(JEVisConstants.Attribute.USER_SYS_ADMIN)) {
                        int isAdmin = rs.getInt(SampleTable.COLUMN_VALUE);
                        if (isAdmin == 1) {
                            user.setSysAdmin(true);
//                            System.out.println("Is Sys Admin");
                        } else {
                            user.setSysAdmin(false);
                        }
                    } else if (attribute.equals(JEVisConstants.Attribute.USER_ENABLED)) {
                        int enabled = rs.getInt(SampleTable.COLUMN_VALUE);
                        if (enabled == 1) {
                            user.setEnabled(true);
//                            System.out.println("is Enabled");
                        } else {
                            user.setEnabled(false);
                        }
                    }
                }
//                if (user.isEnabled()) {
//
//                }

            } else {
                throw new JEVisException("User does not exist or password was wrong", JEVisExceptionCodes.UNAUTHORIZED);
            }

//            ds.setCurrentUserObject(new JEVisUser(object,));
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

    /**
     *
     * @param classes
     * @return
     * @throws JEVisException
     */
    public List<JEVisObject> getObjects(List<JEVisClass> classes) throws JEVisException {
//        System.out.println("getObjects#: " + classes.size());
//        System.out.println("classes: " + Arrays.toString(classes.toArray()));
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
        _ds.addQuery("ObjectTable.getObjects");

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
            logger.debug("getObject.SQl : {}. ", ps);

            while (rs.next()) {

                long objectID = rs.getLong(COLUMN_ID);

                if (!SimpleObjectCache.getInstance().contains(objectID)) {
                    JEVisObjectSQL newObj = new JEVisObjectSQL(_ds, rs);
                    SimpleObjectCache.getInstance().addObject(newObj);
                    newObj.addRelationship(new JEVisRelationshipSQL(_ds, rs));
                    objects.add(newObj);
                } else {
                    JEVisObject cachedObj = SimpleObjectCache.getInstance().getObject(objectID);
                    ((JEVisObjectSQL) cachedObj).addRelationship(new JEVisRelationshipSQL(_ds, rs));
                }

                //without cache
//                boolean isCache = false;
//                for (JEVisObject ob : objects) {
//                    if (ob.getID().equals(rs.getLong(COLUMN_ID))) {
//                        isCache = true;
//                        ((JEVisObjectSQL) ob).addRelationship(new JEVisRelationshipSQL(_ds, rs));
//                    }
//                }
//
//                if (!isCache) {
//                    JEVisObjectSQL newObj = new JEVisObjectSQL(_ds, rs);
//                    newObj.addRelationship(new JEVisRelationshipSQL(_ds, rs));
//                    objects.add(newObj);
//                }
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
        _ds.addQuery("ObjectTable.insertObject");

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
//                    System.out.println("add owner to new object\n\n");
                    for (JEVisRelationship rel : _ds.getRelationshipTable().selectForObject(parent.getID())) {
//                    for (JEVisRelationship rel : parent.getRelationships()) {
//                        System.out.println("rel: " + rel);
//                        System.out.println("startobj: " + rel.getStartObject());
//                        System.out.println("endobj: " + rel.getEndObject());
//                        System.out.println("issametype: " + rel.isType(JEVisConstants.ObjectRelationship.OWNER));

                        if (rel.isType(JEVisConstants.ObjectRelationship.OWNER) && rel.getStartObject().equals(parent)) {
//                            System.out.println("copy this ownership for:" + rs.getLong(1));
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
        _ds.addQuery("ObjectTable.updateObject");

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
        _ds.addQuery("ObjectTable.insertLink");

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

        if (SimpleObjectCache.getInstance().contains(id)) {
            logger.debug("getObject {} is allready in cache", id);
            return SimpleObjectCache.getInstance().getObject(id);
        }

//        JEVisObjectSQL object = null;
        _ds.addQuery("ObjectTable.getObject");

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

                long objectID = rs.getLong(COLUMN_ID);
//                JEVisObjectSQL newObj = new JEVisObjectSQL(_ds, rs);
//                newObj.addRelationship(new JEVisRelationshipSQL(_ds, rs));
//                SimpleObjectCache.getInstance().addClass(newObj);

                if (!SimpleObjectCache.getInstance().contains(objectID)) {
                    JEVisObjectSQL newObj = new JEVisObjectSQL(_ds, rs);
                    newObj.addRelationship(new JEVisRelationshipSQL(_ds, rs));
                    SimpleObjectCache.getInstance().addObject(newObj);
                } else {
                    JEVisObjectSQL sqlObj = (JEVisObjectSQL) SimpleObjectCache.getInstance().getObject(objectID);
                    sqlObj.addRelationship(new JEVisRelationshipSQL(_ds, rs));
                }

//                if (object == null) {
//                    object = new JEVisObjectSQL(_ds, rs);
//                }
//                object.addRelationship(new JEVisRelationshipSQL(_ds, rs));
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

        return SimpleObjectCache.getInstance().getObject(id);
//        _cach.put(id, object);
//        return object;
    }

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
        _ds.addQuery("ObjectTable.deleteObject");

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
        _ds.addQuery("ObjectTable.isUserUnique");

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
