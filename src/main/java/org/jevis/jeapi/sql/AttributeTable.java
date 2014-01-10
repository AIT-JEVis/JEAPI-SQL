/**
 * Copyright (C) 2009 - 2013 Envidatec GmbH <info@envidatec.com>
 *
 * This file is part of JEWebService.
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

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.jevis.jeapi.JEVisAttribute;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisObject;
import org.jevis.jeapi.JEVisType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author florian.simon@envidatec.com
 */
public class AttributeTable {

    public final static String TABLE = "attribute";
    public final static String COLUMN_OBJECT = "object";
    public final static String COLUMN_NAME = "name";
    public final static String COLUMN_MIN_TS = "mints";
    public final static String COLUMN_MAX_TS = "maxts";
    public final static String COLUMN_PERIOD = "period";
    public final static String COLUMN_UNIT = "unit";
    public final static String COLUMN_COUNT = "samplecount";
    private JEVisDataSourceSQL _ds;
    private Connection _connection;
    Logger logger = LoggerFactory.getLogger(AttributeTable.class);

    public AttributeTable(JEVisDataSourceSQL ds) throws JEVisException {
        _ds = ds;
        _connection = _ds.getConnection();
    }

    //TODO: try-catch-finally
    public void insert(JEVisType type, JEVisObject obj) {
        String sql = "insert into " + TABLE
                + "(" + COLUMN_OBJECT + "," + COLUMN_NAME + "," + COLUMN_PERIOD
                + "," + COLUMN_UNIT
                + ") values(?,?,?,?)";

        try {
            PreparedStatement ps = _connection.prepareStatement(sql);

            ps.setLong(1, obj.getID());
            ps.setString(2, type.getName());
            ps.setString(3, "P15m");//TODO implement
            ps.setString(4, "kwh");//TODO implent

            int count = ps.executeUpdate();
//            System.out.println("success");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    //TODO: try-catch-finally
    public void insert(JEVisAttribute att) throws JEVisException {
        insert(att.getType(), att.getObject());
    }

    /**
     *
     * @param object
     * @return
     * @throws SQLException
     * @throws UnsupportedEncodingException
     */
    public List<JEVisAttribute> getAttributes(JEVisObject object) throws JEVisException {
        List<JEVisAttribute> attributes = new ArrayList<JEVisAttribute>();

        String sql = "select * from " + TABLE
                + " where " + COLUMN_OBJECT + "=?";

        try {

            PreparedStatement ps = _connection.prepareStatement(sql);
            ps.setLong(1, object.getID());

            //        System.out.println("SQL: "+ps);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                attributes.add(new JEVisAttributeSQL(_ds, rs));
            }
        } catch (Exception ex) {
            throw new JEVisException("Error while fetching Attributes ", 85675, ex);
        }

        return attributes;
    }

    /**
     *
     * @param object
     * @param attribute
     * @throws SQLException
     * @throws UnsupportedEncodingException
     */
    public void updateAttributeTS(JEVisAttribute att) throws JEVisException {
//        String sql = "select min("+SampleTable.COLUMN_TIMESTAMP+") as min,"
//                + " max("+SampleTable.COLUMN_TIMESTAMP+") as max"
//                + " from " + SampleTable.TABLE
//                + " where "+ SampleTable.COLUMN_OBJECT + "=?"
//                + " and "+SampleTable.COLUMN_ATTRIBUTE+"=?";

        String sql = "update " + TABLE
                + " set "
                + COLUMN_MAX_TS + "=?,"
                + COLUMN_MIN_TS + "=?,"
                + COLUMN_COUNT + "=?"
                + " where " + COLUMN_OBJECT + "=? and " + COLUMN_NAME + "=?";
        PreparedStatement ps = null;

        try {
            ps = _connection.prepareStatement(sql);

            if (att.getTimestampFromLastSample() != null) {
                ps.setTimestamp(1, new Timestamp(att.getTimestampFromLastSample().getMillis()));
            } else {
                ps.setNull(1, Types.TIMESTAMP);
            }

            if (att.getTimestampFromFirstSample() != null) {
                ps.setTimestamp(2, new Timestamp(att.getTimestampFromFirstSample().getMillis()));
            } else {
                ps.setNull(2, Types.TIMESTAMP);
            }


            ps.setLong(3, att.getSampleCount());

            ps.setLong(4, att.getObject().getID());
            ps.setString(5, att.getName());
//            System.out.println("update attribute: \n"+ps);

            //TODO return this??
            int count = ps.executeUpdate();

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("Error while updateing attribute ", 4233, ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }
    }
}
