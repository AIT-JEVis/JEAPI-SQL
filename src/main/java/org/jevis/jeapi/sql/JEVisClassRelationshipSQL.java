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
package org.jevis.jeapi.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisClassRelationship;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisExceptionCodes;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisClassRelationshipSQL implements JEVisClassRelationship {

    private String _start;
    private int _type;
    private String _end;
    private JEVisDataSourceSQL _ds;

    public JEVisClassRelationshipSQL(JEVisDataSourceSQL ds, ResultSet rs) throws JEVisException {
        try {
            _start = rs.getString(ClassRelationTable.COLUMN_START);
            _end = rs.getString(ClassRelationTable.COLUMN_END);
            _type = rs.getInt(ClassRelationTable.COLUMN_TYPE);
            _ds = ds;

        } catch (SQLException ex) {
            ex.printStackTrace();
            throw new JEVisException("Cannot parse Classrelationship", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }
    }

    public JEVisClassRelationshipSQL(JEVisDataSourceSQL ds, String start, String end, int type) {
        this._start = start;
        this._type = type;
        this._end = end;
        this._ds = ds;
    }

    @Override
    public JEVisClass getStart() throws JEVisException {
        return _ds.getJEVisClass(_start);
    }

    @Override
    public JEVisClass getEnd() throws JEVisException {
        return _ds.getJEVisClass(_end);
    }

    @Override
    public int getType() throws JEVisException {
        return _type;
    }

    @Override
    public JEVisClass[] getJEVisClasses() throws JEVisException {
        return new JEVisClass[]{getStart(), getEnd()};
    }

    @Override
    public JEVisClass getOtherClass(JEVisClass jclass) throws JEVisException {
        System.out.println("getOther: " + jclass.getName() + "\n  from " + this);
        if (getStart().getName().equals(jclass.getName())) {
            System.out.println("      end ->" + getEnd().getName());
            return getEnd();
        } else {
            System.out.println("      start->" + getStart().getName());
            return getStart();
        }
    }

    @Override
    public boolean isType(int type) throws JEVisException {
        return (_type == type);
    }

    @Override
    public String toString() {
        return "JEVisClassRelationshipSQL{" + "start=" + _start + ", type=" + _type + ", end=" + _end + '}';
    }
}
