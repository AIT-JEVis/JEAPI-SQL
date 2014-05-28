/**
 * Copyright (C) 2013 - 2014 Envidatec GmbH <info@envidatec.com>
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
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisRelationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class RelationshipTable {

    public final static String TABLE = "relationship";
    public final static String COLUMN_START = "startobject";
    public final static String COLUMN_END = "endobject";
    public final static String COLUMN_TYPE = "relationtype";
    private Connection _connection;
    private JEVisDataSourceSQL _ds;
    private Logger logger = LoggerFactory.getLogger(RelationshipTable.class);

    public RelationshipTable(JEVisDataSourceSQL ds) throws JEVisException {
        _ds = ds;
        _connection = _ds.getConnection();
    }

    public List<JEVisRelationship> select(int type) {
        String sql = "select * from " + TABLE
                + " where " + COLUMN_TYPE + "=?";

        PreparedStatement ps = null;
        List<JEVisRelationship> relations = new LinkedList<JEVisRelationship>();
        _ds.addQuery();

        try {
            ps = _connection.prepareStatement(sql);

            ps.setInt(1, type);
            ResultSet rs = ps.executeQuery();


            while (rs.next()) {
                relations.add(new JEVisRelationshipSQL(_ds, rs));
            }

        } catch (Exception ex) {
            logger.error("Error while selecting relationships from DB: {}", ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }
        return relations;
    }

    //todo: implemet the return save and performant  
    public JEVisRelationship insert(long start, long end, int type) throws JEVisException {

        System.out.println("insert cRel start: " + start + " end: " + end + " type: " + type);

        String sql = "insert into " + TABLE
                + " (" + COLUMN_START + "," + COLUMN_END + "," + COLUMN_TYPE + ")"
                + " values (?,?,?)";

        PreparedStatement ps = null;
        _ds.addQuery();

        try {
            ps = _connection.prepareStatement(sql);
            ps.setLong(1, start);
            ps.setLong(2, end);
            ps.setInt(3, type);

            int count = ps.executeUpdate();
            if (count == 1) {
                return new JEVisRelationshipSQL(_ds, start, end, type);
            } else {
                throw new JEVisException("Could not create the relationship", 1964823);
            }


        } catch (Exception ex) {
            logger.error("Error while inserting relationship into DB: {}", ex.getMessage());
            throw new JEVisException("Could not create the relationship", 1964824, ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }
    }

    public boolean delete(JEVisRelationship rel) throws JEVisException {
        return delete(rel.getStartObject(), rel.getEndObject(), rel.getType());
    }

    public boolean delete(JEVisObject start, JEVisObject end, int type) throws JEVisException {

        String sql = "delete from " + TABLE
                + " where " + COLUMN_START + "=?"
                + " and " + COLUMN_END + "=?"
                + " and " + COLUMN_TYPE + "=?";

        PreparedStatement ps = null;
        _ds.addQuery();

        try {
            ps = _connection.prepareStatement(sql);
            ps.setLong(1, start.getID());
            ps.setLong(2, end.getID());
            ps.setInt(3, type);

            int count = ps.executeUpdate();

            if (count == 1) {
                return true;
            } else {
                return false;
            }


        } catch (Exception ex) {
            logger.error("Error while deleting relationship from DB: {}", ex);
            ex.printStackTrace();
            return false;
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }
    }

    public boolean deleteAll(long id) throws JEVisException {
        return deleteAll(new LinkedList<Long>(Arrays.asList(id)));
    }

    public boolean deleteAll(List<Long> ids) throws JEVisException {

        //TODO make it save with a prepared or so

        PreparedStatement ps = null;
        _ds.addQuery();

        try {
            String in = " IN(";
            for (int i = 0; i < ids.size(); i++) {
                in += ids.get(i);
                if (i != ids.size() - 1) {
                    in += ",";
                }
            }
            in += ")";

            String sql = "delete from " + TABLE
                    + " where " + COLUMN_START + in
                    + " or " + COLUMN_END + in;


            ps = _connection.prepareStatement(sql);

            logger.debug("deleteAll.sql: {}", ps);

            int count = ps.executeUpdate();

            if (count == 1) {
                return true;
            } else {
                return false;
            }


        } catch (Exception ex) {
            logger.error("Error while deleting relationship from DB: {}", ex.getMessage());
            ex.printStackTrace();
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
