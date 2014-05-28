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
package org.jevis.api.sql;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisConstants;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisExceptionCodes;
import org.jevis.api.JEVisSample;
import org.joda.time.DateTime;

/**
 *
 * @author Florian Simon<florian.simon@envidatec.com>
 */
public class SampleTable {

    public final static String TABLE = "sample";
    public final static String COLUMN_VALUE = "value";
    public final static String COLUMN_ATTRIBUTE = "attribute";
    public final static String COLUMN_TIMESTAMP = "timestamp";//rename into ts?
    public final static String COLUMN_INSERT_TIMESTAMP = "insertts";
    public final static String COLUMN_OBJECT = "object";
    public final static String COLUMN_MANID = "manid";
    public final static String COLUMN_NOTE = "note";
    public final static String COLUMN_FILE = "file";
    public final static String COLUMN_FILE_NAME = "filename";
    private Connection _connection;
    private JEVisDataSourceSQL _ds;

    public SampleTable(JEVisDataSourceSQL ds) throws JEVisException {
        _ds = ds;
        _connection = _ds.getConnection();
    }

    public int insertSamples(JEVisAttribute attribute, List<JEVisSample> samples) throws JEVisException {
        int perChunk = 100000;// care if value is bigger sql has a limit per transaktion. 1mio is teste only with small ints    
        int count = 0;
        for (int i = 0; i < samples.size(); i += perChunk) {
            if ((i + perChunk) < samples.size()) {
                List<JEVisSample> chunk = samples.subList(i, i + perChunk);
                count += insertSamplesChunk(attribute, chunk);
            } else {
                List<JEVisSample> chunk = samples.subList(i, samples.size());
                count += insertSamplesChunk(attribute, chunk);
                break;
            }
        }
        return count;
    }

//    /**
//     * Handel special Attributes like the Password filed
//     *
//     * @param att
//     * @return
//     */
//    private boolean isAttributeOK(JEVisAttribute att, boolean write) throws JEVisException {
//        //Passwort cannot be see by anybody, can only be set by user an admin
//        if (att.getName().equalsIgnoreCase("Password")
//                || att.getObject().getJEVisClass().getName().equalsIgnoreCase(JEVisConstants.CLASS_USER)) {
//            if (write) {
//                //Admins and same can set the password. The check if the Admin has access to the object will be handelt elsewhere
//                if (_ds.getCurrentUser().isAdministrator() || _ds.getCurrentUser().getName().equals(att.getObject().getName())) {
//                    return true;
//                } else {
//                    throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
//                }
//            } else {
//                return false;
//            }
//        }
//
//        return true;
//    }
    /**
     * TODO: batch the insert because mysql has a limit for a request
     * "max_allowed_packet=32M"
     *
     * @param object
     * @param attribute
     * @param samples
     */
    private int insertSamplesChunk(JEVisAttribute attribute, List<JEVisSample> samples) throws JEVisException {
        String sql = "INSERT IGNORE into " + TABLE
                + "(" + COLUMN_OBJECT + "," + COLUMN_ATTRIBUTE + "," + COLUMN_TIMESTAMP
                + "," + COLUMN_VALUE + "," + COLUMN_MANID + "," + COLUMN_NOTE + "," + COLUMN_INSERT_TIMESTAMP
                + "," + COLUMN_FILE_NAME + "," + COLUMN_FILE
                + ") VALUES ";

//        System.out.println("SQL raw: "+sql);
        PreparedStatement ps = null;
        int count = 0;

        try {
            //SringBuilder is faster then a sql batch script
            StringBuilder build = new StringBuilder(sql);

            for (int i = 0; i < samples.size(); i++) {
                build.append("(?,?,?,?,?,?,?,?,?)");
                if (i < samples.size() - 1) {
                    build.append(", ");
                } else {
                    build.append(";");
                }
            }

            ps = _connection.prepareStatement(build.toString());
//            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));//care tor TZ?
            Calendar cal = Calendar.getInstance();//care tor TZ?
            long now = cal.getTimeInMillis();

            int p = 0;
            for (int i = 0; i < samples.size(); i++) {
                JEVisSample sample = samples.get(i);
                ps.setLong(++p, attribute.getObject().getID());
                ps.setString(++p, attribute.getName());
                ps.setTimestamp(++p, new Timestamp(sample.getTimestamp().getMillis()));

                if (sample.getAttribute().getPrimitiveType() == JEVisConstants.PrimitiveType.PASSWORD_PBKDF2) {
                    //Passwords will be stored as Saled Hash
                    ps.setString(++p, PasswordHash.createHash(sample.getValue().toString()));
                } else if (sample.getAttribute().getPrimitiveType() == JEVisConstants.PrimitiveType.FILE) {
                    ps.setNull(++p, Types.VARCHAR);
                } else if (sample.getAttribute().getPrimitiveType() == JEVisConstants.PrimitiveType.BOOLEAN) {
                    ps.setBoolean(++p, sample.getValueAsBoolean());
                } else if (sample.getAttribute().getPrimitiveType() == JEVisConstants.PrimitiveType.SELECTION) {
                    ps.setLong(++p, sample.getValueAsLong());
                } else if (sample.getAttribute().getPrimitiveType() == JEVisConstants.PrimitiveType.MULTI_SELECTION) {
                    ps.setString(++p, sample.getValueAsString());
                } else {
                    ps.setString(++p, sample.getValue().toString());
                }


//                ps.setString(++p, sample.getManipulation().toString());
                ps.setNull(++p, Types.INTEGER);
                ps.setString(++p, sample.getNote());
                ps.setTimestamp(++p, new Timestamp(now));


                if (sample.getAttribute().getPrimitiveType() == JEVisConstants.PrimitiveType.FILE) {
//                    System.out.println("Filename: "+sample.getValueAsFile().getFilename());
//                    System.out.println("byte[]: "+sample.getValueAsFile().getBytes());
                    ps.setString(++p, sample.getValueAsFile().getFilename());
                    ps.setBytes(++p, sample.getValueAsFile().getBytes());
//                    ps.setString(++p, sample.getFilename());
//                    ps.setBytes(++p, sample.valueToByte());
                } else {
                    ps.setNull(++p, Types.BLOB);
                    ps.setNull(++p, Types.VARCHAR);
                }
            }
            //System.out.println("SamplDB.putSample SQL: \n" + ps);


            count = ps.executeUpdate();
            if (count > 0) {
                _ds.getAttributeTable().updateAttributeTS(attribute);
            }

            return count;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("Error while inserting Sample ", 4234, ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }

    }

    public List<JEVisSample> getSamples(JEVisAttribute att, DateTime from, DateTime until) throws JEVisException {
        List<JEVisSample> samples = new ArrayList<JEVisSample>();

        String sql = "select * from " + TABLE
                + " where " + COLUMN_OBJECT + "=?"
                + " and " + COLUMN_ATTRIBUTE + "=?";


        if (from != null) {
            sql += " and " + COLUMN_TIMESTAMP + ">=?";
        }
        if (until != null) {
            sql += " and " + COLUMN_TIMESTAMP + "<=?";
        }
        sql += " order by " + COLUMN_TIMESTAMP;

        PreparedStatement ps = null;

        try {

            ps = _connection.prepareStatement(sql);
            int pos = 1;

            ps.setLong(pos++, att.getObject().getID());
            ps.setString(pos++, att.getName());
            if (from != null) {
                ps.setTimestamp(pos++, new Timestamp(from.getMillis()));
            }
            if (until != null) {
                ps.setTimestamp(pos++, new Timestamp(until.getMillis()));
            }


            System.out.println("SQL: " + ps);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                samples.add(new JEVisSampleSQL(_ds, rs, att));
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("Error while select samples", 723547, ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) { /*ignored*/ }
            }
        }
        return samples;
    }

    public boolean deleteAllSamples(JEVisAttribute att) throws JEVisException {
        String sql = "delete from " + TABLE
                + " where " + COLUMN_ATTRIBUTE + "=?"
                + " and " + COLUMN_OBJECT + "=?";

        PreparedStatement ps = null;

        try {
            ps = _connection.prepareStatement(sql);
            ps.setString(1, att.getName());
            ps.setLong(2, att.getObject().getID());

            if (ps.executeUpdate() > 0) {
                return true;
            } else {
                return false;
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

    public boolean deleteSamples(JEVisAttribute att, DateTime from, DateTime until) throws JEVisException {
        String sql = "delete from " + TABLE
                + " where " + COLUMN_ATTRIBUTE + "=?"
                + " and " + COLUMN_OBJECT + "=?"
                + " and " + COLUMN_TIMESTAMP + ">=?"
                + " and " + COLUMN_TIMESTAMP + "<=?";

        PreparedStatement ps = null;

        try {
            ps = _connection.prepareStatement(sql);
            ps.setString(1, att.getName());
            ps.setLong(2, att.getObject().getID());
            ps.setTimestamp(3, new Timestamp(from.getMillis()));
            ps.setTimestamp(4, new Timestamp(until.getMillis()));

            if (ps.executeUpdate() > 0) {
                return true;
            } else {
                return false;
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

    public List<JEVisSample> getAll(JEVisAttribute att) throws SQLException, UnsupportedEncodingException, JEVisException {
        List<JEVisSample> samples = new ArrayList<JEVisSample>();

        String sql = "select * from " + TABLE
                + " where " + COLUMN_OBJECT + "=?"
                + " and " + COLUMN_ATTRIBUTE + "=?"
                + " order by " + COLUMN_TIMESTAMP;


        PreparedStatement ps = _connection.prepareStatement(sql);
        ps.setLong(1, att.getObject().getID());
        ps.setString(2, att.getName());

        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            samples.add(new JEVisSampleSQL(_ds, rs, att));
        }

        return samples;
    }

    public JEVisSample getLatest(JEVisAttribute att) throws SQLException, UnsupportedEncodingException, JEVisException {
        JEVisSample sample = null;
//        System.out.println("SampleTable.getLatest");

        String sql = "select * from " + TABLE
                + " where " + COLUMN_OBJECT + "=?"
                + " and " + COLUMN_ATTRIBUTE + "=?"
                + " and " + COLUMN_TIMESTAMP + "=?"
                + " limit 1";

//        System.out.println("Lasts: "+att.getTimestampFromLastSample());
//        System.out.println("Lasts: "+att.getTimestampFromLastSample().getMillis());
//        System.out.println("Lasts: "+(new Timestamp(att.getTimestampFromLastSample().getMillis())));

        PreparedStatement ps = _connection.prepareStatement(sql);
        ps.setLong(1, att.getObject().getID());
        ps.setString(2, att.getName());
        ps.setTimestamp(3, new Timestamp(att.getTimestampFromLastSample().getMillis()));
//        System.out.println("SQL: "+ps);
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            sample = new JEVisSampleSQL(_ds, rs, att);
        }

        return sample;
    }
}
