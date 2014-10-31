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

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.measure.unit.UnitFormat;
import org.jevis.commons.unit.UnitManager;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisType;

/**
 *
 * @author flo
 */
public class TypeTable {

    public final static String TABLE = "type";
    public final static String COLUMN_CLASS = "jevisclass";
    public final static String COLUMN_NAME = "name";
    public final static String COLUMN_DISPLAY_TYPE = "displaytype";
    public final static String COLUMN_PRIMITIV_TYPE = "primitivtype";
//    public final static String COLUMN_INTERPRETATION="interpretation";//TODO: find a better name...
    public final static String COLUMN_DEFAULT_UNIT = "defaultunit";
    public final static String COLUMN_GUI_WEIGHT = "guiposition";
    public final static String COLUMN_DESCRIPTION = "description";
    public final static String COLUMN_VALIDITY = "validity";
    public final static String COLUMN_VALUE = "value";
    public final static String COLUMN_ALT_SYMBOL = "altsymbol";

    private final JEVisDataSourceSQL _ds;
    private final Connection _connection;

    public TypeTable(JEVisDataSourceSQL ds) throws JEVisException {
        _ds = ds;
        _connection = _ds.getConnection();
    }

    public boolean update(JEVisType type, String originalName) {
        try {
            String sql = "update " + TABLE
                    + " set " + COLUMN_DESCRIPTION + "=?," + COLUMN_NAME + "=?,"
                    + COLUMN_CLASS + "=?," + COLUMN_DISPLAY_TYPE + "=?,"
                    + COLUMN_PRIMITIV_TYPE + "=?," + COLUMN_DEFAULT_UNIT + "=?,"
                    + COLUMN_GUI_WEIGHT + "=?," + COLUMN_VALIDITY + "=?,"
                    + COLUMN_VALUE + "=?," + COLUMN_ALT_SYMBOL + "=?"
                    + " where " + COLUMN_NAME + "=? and " + COLUMN_CLASS + "=?";

            PreparedStatement ps = _connection.prepareStatement(sql);

            ps.setString(1, type.getDescription());
            ps.setString(2, type.getName());

            ps.setString(3, type.getJEVisClass().getName());
            ps.setString(4, type.getGUIDisplayType());

            ps.setInt(5, type.getPrimitiveType());
            System.out.println("sql.unit: " + type.getUnit());
            ps.setString(6, type.getUnit().toString());
//            ps.setString(6, UnitManager.getInstance().formate(type.getUnit()));

            ps.setInt(7, type.getGUIPosition());
            ps.setInt(8, type.getValidity());

            ps.setString(9, type.getConfigurationValue());
            ps.setString(10, type.getAlternativSymbol());

            ps.setString(11, originalName);
            ps.setString(12, type.getJEVisClass().getName());

            System.out.println("sql.type.update: " + ps);
            int res = ps.executeUpdate();

            //Check if the name changed, if yes we have to change all existing JEVisObjects.....do we want that?
            if (res == 1) {
                if (!type.equals(originalName)) {
                    //------------------ Update exintings Attributes

                    String updateObject = "update " + AttributeTable.TABLE + "," + ObjectTable.TABLE
                            + " set " + AttributeTable.TABLE + "." + AttributeTable.COLUMN_NAME + "=?"
                            + " where " + AttributeTable.TABLE + "." + AttributeTable.COLUMN_NAME + "=?"
                            + " and " + AttributeTable.TABLE + "." + AttributeTable.COLUMN_OBJECT + "=" + ObjectTable.TABLE + "." + ObjectTable.COLUMN_ID
                            + " and " + ObjectTable.TABLE + "." + ObjectTable.COLUMN_CLASS + "=?";

                    PreparedStatement ps2 = _connection.prepareStatement(updateObject);
                    ps2.setString(1, type.getName());
                    ps2.setString(2, originalName);
                    ps2.setString(3, type.getJEVisClass().getName());

//                    System.out.println("SQL updateAttriutes: \n"+ps2);
                    int res2 = ps2.executeUpdate();
                    if (res2 > 0) {
//                        System.out.println("updated " + res2 + " JEVisAttributes");
                    }

                }

                return true;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return false;

    }

    public boolean detele(JEVisType type) {
        String sql = "delete from " + TABLE
                + " where " + COLUMN_CLASS + "=?"
                + " and " + COLUMN_NAME + "=?";
        PreparedStatement ps = null;

        try {
            ps = _connection.prepareStatement(sql);
            ps.setString(1, type.getJEVisClass().getName());
            ps.setString(2, type.getName());

            System.out.println("Delete Type: " + type);

            if (ps.executeUpdate() == 1) {
                System.out.println("true");
                return true;
            } else {
                System.out.println("false");
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

    public boolean insert(JEVisClass jclass, String newType) {
        try {
            String sql = "insert into " + TABLE
                    + "(" + COLUMN_NAME + "," + COLUMN_CLASS + ")"
                    + " values(?,?)";

            PreparedStatement ps = _connection.prepareStatement(sql);
            ps.setString(1, newType);
            ps.setString(2, jclass.getName());

            int count = ps.executeUpdate();

            if (count > 0) {
                return true;
            } else {
                return false;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            return false;
        }
    }

    /**
     *
     * @param object
     * @return
     * @throws SQLException
     * @throws UnsupportedEncodingException
     */
    public List<JEVisType> getAll(JEVisClass jclass) throws SQLException, UnsupportedEncodingException, JEVisException {
        List<JEVisType> types = new ArrayList<JEVisType>();

        String sql = "select * from " + TABLE
                + " where " + COLUMN_CLASS + "=?";

        PreparedStatement ps = _connection.prepareStatement(sql);
        ps.setString(1, jclass.getName());

        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            types.add(new JEVisTypeSQL(_ds, rs, jclass));
        }

        return types;
    }
}
