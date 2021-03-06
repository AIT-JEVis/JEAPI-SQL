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
import java.util.ArrayList;
import java.util.List;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisClassRelationship;
import org.jevis.api.JEVisException;

/**
 *
 * @author Florian Simon<florian.simon@envidatec.com>
 */
public class ClassRelationTable {

    public final static String TABLE = "classrelationship";
    public final static String COLUMN_START = "startclass";
//    public final static String COLUMN_INHERITANCE="inheritance";
    public final static String COLUMN_END = "endclass";
    public final static String COLUMN_TYPE = "type";
    private Connection _connection;
    private JEVisDataSourceSQL _ds;

    public ClassRelationTable(JEVisDataSourceSQL ds) throws JEVisException {
        _connection = ds.getConnection();
        _ds = ds;
    }

    /**
     *
     * @param rel
     * @return
     * @throws JEVisException
     */
    public boolean delete(JEVisClassRelationship rel) throws JEVisException {
        String sql = "delete from " + TABLE
                + " where " + COLUMN_START + "=?"
                + " and " + COLUMN_END + "=?"
                + " and " + COLUMN_TYPE + "=?";

        PreparedStatement ps = null;
        _ds.addQuery("ClassRelationTable.rel");

        try {

            ps = _connection.prepareStatement(sql);

            //TODO: remove this workaround
            //sometime the related classes are already delete the relationship (most likely) but the Object is still in the memory
            //The problem is related to the cache which should be removed in the future
            if (rel.getStart() == null || rel.getEnd() == null) {
                return false;
            }

            ps.setString(1, rel.getStart().getName());
            ps.setString(2, rel.getEnd().getName());
            ps.setInt(3, rel.getType());

            System.out.println("Delete.classrelationship: " + ps.toString());
            int count = ps.executeUpdate();
            if (count == 1) {
                return true;
            } else {
                return false;
            }

        } catch (Exception ex) {
            throw new JEVisException("Could not delete new ClassRelationship", 578246, ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }

    }

    /**
     *
     * @param start
     * @param end
     * @param type
     * @return
     * @throws JEVisException
     */
    public JEVisClassRelationship insert(JEVisClass start, JEVisClass end, int type) throws JEVisException {
        String sql = "insert into " + TABLE + " (" + COLUMN_START + "," + COLUMN_END + "," + COLUMN_TYPE + ") "
                + " values(?,?,?)";
        PreparedStatement ps = null;

        try {
            _ds.addQuery("ClassRelationTable.insert");
            ps = _connection.prepareStatement(sql);
            ps.setString(1, start.getName());
            ps.setString(2, end.getName());
            ps.setInt(3, type);

//            System.out.println("JEVisClassRelationship.insert: " + ps);
            int count = ps.executeUpdate();
            if (count > 0) {
                //TODO: maybe fetch from Db to be save
                return new JEVisClassRelationshipSQL(_ds, start.getName(), end.getName(), type);
            } else {
                throw new JEVisException("Could not insert new ClassRelationship", 578245);
            }

        } catch (Exception ex) {
            throw new JEVisException("Could not insert new ClassRelationship", 578246, ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }

    }

    /**
     *
     * @param jclass
     * @return
     * @throws JEVisException
     */
    public List<JEVisClassRelationship> get(JEVisClass jclass) throws JEVisException {
        List<JEVisClassRelationship> relations = new ArrayList<JEVisClassRelationship>();
//        String sql = "select * from " + TABLE
//                + " where " + COLUMN_START + "=?"
//                + " or " + COLUMN_END + "=?";

        //saver this will exclude not existion classes
        String sql = "select distinct " + TABLE + ".* from " + TABLE
                + " left join " + ClassTable.TABLE + " c1 on " + TABLE + "." + COLUMN_START + "=c1." + ClassTable.COLUMN_NAME
                + " left join " + ClassTable.TABLE + " c2 on " + TABLE + "." + COLUMN_END + "=c2." + ClassTable.COLUMN_NAME
                + " where (" + COLUMN_START + "=?" + " or " + COLUMN_END + "=? )"
                + " and c1." + ClassTable.COLUMN_NAME + " is not null "
                + " and c2." + ClassTable.COLUMN_NAME + " is not null ";

        _ds.addQuery("ClassRelationTable.get");
        PreparedStatement ps = null;
        try {

            ps = _connection.prepareStatement(sql);
            ps.setString(1, jclass.getName());
            ps.setString(2, jclass.getName());

//            System.out.println("CR.sql: " + ps);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                relations.add(new JEVisClassRelationshipSQL(_ds, rs));
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("Error while fetching ClassRelationship", 7390562, ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }
        return relations;
    }
}
