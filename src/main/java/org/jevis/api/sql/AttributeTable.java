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
package org.jevis.api.sql;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisOption;
import org.jevis.api.JEVisType;
import org.jevis.commons.json.JsonOption;
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
//    public final static String COLUMN_PERIOD = "period";//depricated
//    public final static String COLUMN_UNIT = "unit";//depricated
    public final static String COLUMN_COUNT = "samplecount";
//    public final static String COLUMN_ALT_SYMBOL = "altsymbol";

    public final static String COLUMN_INPUT_UNIT = "inputunit";
    public final static String COLUMN_INPUT_RATE = "inputrate";
    public final static String COLUMN_DISPLAY_UNIT = "displayunit";
    public final static String COLUMN_DISPLAY_RATE = "displayrate";
    public final static String COLUMN_OPTION = "opt";//option and options are already sql keywords

    private final JEVisDataSourceSQL _ds;
    private final Connection _connection;
    Logger logger = LoggerFactory.getLogger(AttributeTable.class);

    public AttributeTable(JEVisDataSourceSQL ds) throws JEVisException {
        _ds = ds;
        _connection = _ds.getConnection();
    }

    //TODO: try-catch-finally
    public void insert(JEVisType type, JEVisObject obj) {
        System.out.println("AttributeTable.insert");
        String sql = "insert into " + TABLE
                + " (" + COLUMN_OBJECT + "," + COLUMN_NAME
                + "," + COLUMN_DISPLAY_UNIT + "," + COLUMN_INPUT_UNIT
                + ") values(?,?,?,?)";

        try {
            PreparedStatement ps = _connection.prepareStatement(sql);
            _ds.addQuery("AttributeTable.insert");

            ps.setLong(1, obj.getID());
            ps.setString(2, type.getName());

            String unitJSON = "";
            try {
                unitJSON = type.getUnit().toJSON();
            } catch (Exception ex) {

            }

            //DisplayUnit
            ps.setString(3, unitJSON);
            ps.setString(4, unitJSON);

            System.out.println("AttributeTable.insert: " + ps);
            int count = ps.executeUpdate();
            System.out.println("AttributeTable.insert: " + count);
            System.out.println("success");
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
     * @throws JEVisException
     */
    public List<JEVisAttribute> getAttributes(JEVisObject object) throws JEVisException {
        List<JEVisAttribute> attributes = new ArrayList<JEVisAttribute>();

        String sql = "select * from " + TABLE
                + " where " + COLUMN_OBJECT + "=?";

        try {
            _ds.addQuery("AttributeTable.getAttributes");
            PreparedStatement ps = _connection.prepareStatement(sql);
            ps.setLong(1, object.getID());

//            System.out.println("SQL: " + ps);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                attributes.add(new JEVisAttributeSQL(_ds, rs));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("Error while fetching Attributes ", 85675, ex);

        }

        return attributes;
    }

    /**
     *
     * @param att
     * @throws JEVisException
     */
    public void updateAttributeTS(JEVisAttribute att) throws JEVisException {
//        System.out.println("Update attribute: " + att.getName());
        String sql = "update " + TABLE
                + " set "
                + COLUMN_MAX_TS + "=(select max(" + SampleTable.COLUMN_TIMESTAMP + ") from " + SampleTable.TABLE + " where " + SampleTable.COLUMN_OBJECT + "=?" + " and " + SampleTable.COLUMN_ATTRIBUTE + "=? limit 1),"
                + COLUMN_MIN_TS + "=(select min(" + SampleTable.COLUMN_TIMESTAMP + ") from " + SampleTable.TABLE + " where " + SampleTable.COLUMN_OBJECT + "=?" + " and " + SampleTable.COLUMN_ATTRIBUTE + "=? limit 1),"
                + COLUMN_COUNT + "=(select count(*) from " + SampleTable.TABLE + " where " + SampleTable.COLUMN_OBJECT + "=?" + " and " + SampleTable.COLUMN_ATTRIBUTE + "=? limit 1), "
                + COLUMN_DISPLAY_UNIT + "=?," + COLUMN_INPUT_UNIT + "=?," + COLUMN_DISPLAY_RATE + "=?," + COLUMN_INPUT_RATE + "=?, " + COLUMN_OPTION + "=?"
                + " where " + COLUMN_OBJECT + "=? and " + COLUMN_NAME + "=?";

        PreparedStatement ps = null;

        try {
            _ds.addQuery("AttributeTable.updateAttributeTS");
            ps = _connection.prepareStatement(sql);

            //unit
//            ps.setString(1, att.getAlternativSymbol());
//            ps.setString(2, att.getDisplayUnit().toString());
            //1. sub (max)
            ps.setLong(1, att.getObject().getID());
            ps.setString(2, att.getName());

            //2.sub (min)
            ps.setLong(3, att.getObject().getID());
            ps.setString(4, att.getName());

            //3.sub (count)
            ps.setLong(5, att.getObject().getID());
            ps.setString(6, att.getName());

            ps.setString(7, att.getDisplayUnit().toJSON());
            ps.setString(8, att.getInputUnit().toJSON());
            ps.setString(9, att.getDisplaySampleRate().toString());
            ps.setString(10, att.getInputSampleRate().toString());

            if (att.getOptions() != null && !att.getOptions().isEmpty()) {
                Gson gson = new Gson();

                Type listType = new TypeToken<ArrayList<JsonOption>>() {
                }.getType();
                List<JsonOption> opts = new ArrayList<JsonOption>();
                for (JEVisOption option : att.getOptions()) {
                    JsonOption jopt = new JsonOption(option);
                    opts.add(jopt);

                }
                ps.setString(11, gson.toJson(opts, listType));

            } else {
                ps.setString(11, "");
            }

            //where
            ps.setLong(12, att.getObject().getID());
            ps.setString(13, att.getName());

//            System.out.println("Attribute.update().sql: " + ps.toString());
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
