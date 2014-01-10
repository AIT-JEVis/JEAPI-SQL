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
package org.jevis.jeapi.sql;

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
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisConstants;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisExceptionCodes;
import org.jevis.jeapi.JEVisObject;
import org.jevis.jeapi.JEVisRelationship;
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
    public final static String COLUMN_PARENT = "parent";
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

        try {

            ps = _connection.prepareStatement(sql);
            ps.setString(1, name);
            ps.setString(2, JEVisConstants.Class.USER);
            ps.setString(3, JEVisConstants.Attribute.USER_PASSWORD);

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
                + " left join " + RelationshipTable.TABLE
                + " on " + TABLE + "." + COLUMN_ID + "=" + RelationshipTable.TABLE + "." + RelationshipTable.COLUMN_END
                + " where " + TABLE + "." + COLUMN_CLASS + " in (";

        PreparedStatement ps = null;
        ResultSet rs = null;
        List<JEVisObject> objects = new ArrayList<JEVisObject>();

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
            logger.debug("getObject.SQl : {}. ", ps);

            for (int i = 0; i < classes.size(); i++) {
                JEVisClass cl = classes.get(i);
                ps.setString(i + 1, cl.getName());//index start with 1 for prepareStatements...
            }


            rs = ps.executeQuery();

            while (rs.next()) {
                objects.add(new JEVisObjectSQL(_ds, rs));

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

    public JEVisObject insertObject(String name, String jclass, JEVisObject parent, long group) throws JEVisException {
        String sql = "insert into " + TABLE
                + "(" + COLUMN_NAME + "," + COLUMN_CLASS + "," + COLUMN_PARENT + "," + COLUMN_GROUP + ")"
                + " values(?,?,?,?)";
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = _connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setString(2, jclass);
            ps.setLong(3, parent.getID());
            ps.setLong(4, 0);
//            System.out.println("putObject: " + ps);

            int count = ps.executeUpdate();
            if (count == 1) {
                rs = ps.getGeneratedKeys();

                if (rs.next()) {
                    _ds.getRelationshipTable().insert(rs.getLong(1), parent.getID(), JEVisConstants.Relationship.PARENT);
                    for (JEVisRelationship rel : parent.getRelationships()) {
                        if (rel.isType(JEVisConstants.Relationship.OWNER)) {
                            _ds.getRelationshipTable().insert(rs.getLong(1), rel.getEndObject().getID(), JEVisConstants.Relationship.OWNER);
                        }
                    }

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
            if (rs != null) {
                try {
                    rs.close();
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
                + "(" + COLUMN_NAME + "," + COLUMN_PARENT + "," + COLUMN_GROUP + "," + COLUMN_LINK + "," + COLUMN_CLASS + ")"
                + " values(?,?,?,?,?)";
        JEVisObject newObject = null;

        try {
            PreparedStatement ps = _connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
//            PreparedStatement ps = _connection.prepareStatement(sql);
            ps.setString(1, name);
            ps.setLong(2, linkParent.getID());
            ps.setLong(3, 0);//TODO replace this is not needet anymore
            ps.setLong(4, linkedObject.getID());
            ps.setString(5, "Link");

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

    public List<JEVisObject> getChildren(JEVisObject obj) throws JEVisException {
        ResultSet rs = null;
        List<JEVisObject> children = new ArrayList<JEVisObject>();

        String sql = "select o.*"
                + ",r.*"
                + " from " + TABLE + " o"
                + " left join " + RelationshipTable.TABLE + " r"
                + " on o." + COLUMN_ID + "=" + "r." + RelationshipTable.COLUMN_START
                + " or o." + COLUMN_ID + "=" + "r." + RelationshipTable.COLUMN_END
                + " where  o." + COLUMN_PARENT + "=?"
                + " and o." + COLUMN_DELETE + " is null";

        try {
            PreparedStatement ps = _connection.prepareStatement(sql);
            ps.setLong(1, obj.getID());


//        System.out.println("getObject.sql: "+ps);
            rs = ps.executeQuery();

            Map<Long, JEVisObjectSQL> _tmp = new HashMap<Long, JEVisObjectSQL>();

            while (rs.next()) {
                long id = rs.getLong(COLUMN_ID);
                if (!_tmp.containsKey(id)) {
                    _tmp.put(id, new JEVisObjectSQL(_ds, rs));
                }
                _tmp.get(id).addRelationship(new JEVisRelationshipSQL(_ds, rs));

            }
            children = new LinkedList<JEVisObject>(_tmp.values());

        } catch (SQLException ex) {
            throw new JEVisException("Error while select Child objects", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ex) {
                    logger.debug("Error while closing DB connection: {}. ", ex);
                }
            }

        }


        return children;
    }

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
    private List<Long> getAllChildren(long id) throws JEVisException {
        String sql = "select " + COLUMN_ID + "," + COLUMN_PARENT + " from " + TABLE;

        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<Long, Long> allID = new HashMap<Long, Long>();
        List<Long> children = new ArrayList<Long>();

        try {
            ps = _connection.prepareStatement(sql);

            rs = ps.executeQuery();


            while (rs.next()) {
//                System.out.println("add to map: ID:"+rs.getLong(COLUMN_ID)+"  Parent:"+rs.getLong(COLUMN_PARENT));
                allID.put(rs.getLong(COLUMN_ID), rs.getLong(COLUMN_PARENT));

            }

            findChildren(children, allID, id);
            return children;


        } catch (Exception ex) {
            ex.printStackTrace();
            //TODO error handling
            return children;
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) { /*ignored*/ }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }
    }

    public boolean deleteObject(long id) throws JEVisException {
        String sql = "update " + TABLE
                + " set " + COLUMN_DELETE + "=?"
                + " where " + COLUMN_ID + " IN(?";
        PreparedStatement ps = null;

        try {
//            System.out.println("find children to delete");
            List<Long> children = getAllChildren(id);

            //TODO use string builder
            for (Long cid : children) {
                sql += ",?";
            }
            sql += ")";
            Calendar now = new GregorianCalendar();

            ps = _connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            ps.setTimestamp(1, new Timestamp(now.getTimeInMillis()));
            ps.setLong(2, id);
            for (int i = 0; i < children.size(); i++) {
                ps.setLong(i + 3, children.get(i));
            }

            int count = ps.executeUpdate();

            if (count > 0) {
                _cach.remove(id);

                _ds.getRelationshipTable().deleteAll(id);

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
}
