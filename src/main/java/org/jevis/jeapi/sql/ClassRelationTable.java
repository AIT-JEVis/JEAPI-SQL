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
import java.util.ArrayList;
import java.util.List;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisException;

/**
 *
 * @author Florian Simon<florian.simon@envidatec.com>
 */
public class ClassRelationTable {

    public final static String TABLE = "classrelation";
    public final static String COLUMN_CLASS = "class";
//    public final static String COLUMN_INHERITANCE="inheritance";
    public final static String COLUMN_OKPARENT = "validparent";
    private Connection _connection;
    private JEVisDataSourceSQL _ds;

    public ClassRelationTable(JEVisDataSourceSQL ds) throws JEVisException {
        _connection = ds.getConnection();
        _ds = ds;
    }

// 
//    public boolean putInherit(JEVisClass jclass, JEVisClass inherit ){
//        try{
//            String sql = "insert into "+ TABLE+" ("+COLUMN_CLASS+","+COLUMN_INHERITANCE+") "
//                    + " values(?,?)"
//                    + " on duplicate key update "+COLUMN_CLASS+"=?,"+COLUMN_INHERITANCE+"=?";
//            
//            PreparedStatement ps = _connection.prepareStatement(sql);            
//            ps.setString(1, jclass.getName());
//            ps.setString(2, inherit.getName());
//            ps.setString(3, jclass.getName());
//            ps.setString(4, inherit.getName());
//            
//            System.out.println("SQL: "+ps);
//            int count = ps.executeUpdate();
//            if(count>0){
//                return true;  
//            }else{
//                return false;
//            }
//            
//            
//        }catch(Exception ex){
//            return false;
//        }
//        
//        
//    }
//    
    public boolean putVaildParent(JEVisClass jclass, JEVisClass inherit) {
        try {
            String sql = "insert into " + TABLE + " (" + COLUMN_CLASS + "," + COLUMN_OKPARENT + ") "
                    + " values(?,?)"
                    + " on duplicate key update " + COLUMN_CLASS + "=?," + COLUMN_OKPARENT + "=?";

            PreparedStatement ps = _connection.prepareStatement(sql);
            ps.setString(1, jclass.getName());
            ps.setString(2, inherit.getName());
            ps.setString(3, jclass.getName());
            ps.setString(4, inherit.getName());

            System.out.println("SQL: " + ps);
            int count = ps.executeUpdate();
            if (count > 0) {
                return true;
            } else {
                return false;
            }


        } catch (Exception ex) {
            return false;
        }


    }

    public List<Relation> get(JEVisClass jclass) throws SQLException, JEVisException {
        List<Relation> relations = new ArrayList<Relation>();
        String sql = "select * from " + TABLE + " where " + COLUMN_CLASS + "=?";

        PreparedStatement ps = _connection.prepareStatement(sql);
        ps.setString(1, jclass.getName());

        ResultSet rs = ps.executeQuery();


        while (rs.next()) {
            relations.add(new Relation(_ds, rs, jclass));
        }

        rs.close();

        return relations;
    }
}
