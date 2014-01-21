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
import java.util.LinkedList;
import java.util.List;
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
    private JEVisClass _inHerited = null;
    private JEVisClass _heir = null;
    private boolean _isInHereted = false;

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
//        if (_end.equals(_heir)) {
//            return replaceHerit(_ds.getJEVisClass(_start));
//        } else {
//            return _ds.getJEVisClass(_start);
//        }

//        JEVisClass start = _ds.getJEVisClass(_start);
//        if (_isInHereted && start.equals(_inHerited)) {
//            start = _heir;
//        }
//        return start;
        return _ds.getJEVisClass(_start);
    }

    @Override
    public JEVisClass getEnd() throws JEVisException {
//        if (_start.equals(_heir)) {
//            return replaceHerit(_ds.getJEVisClass(_end));
//        } else {
//            return _ds.getJEVisClass(_end);
//        }


//        JEVisClass end = _ds.getJEVisClass(_end);
//        if (_isInHereted && end.equals(_inHerited)) {
//            end = _heir;
//        }
//        return end;
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
        if (getStart().getName().equals(jclass.getName())) {
            return getEnd();
        } else {
            return getStart();
        }
    }

    @Override
    public boolean isType(int type) throws JEVisException {
        return (_type == type);
    }

    @Override
    public String toString() {
        String end = "error";
        String start = "error";
        try {
            start = getStart().getName();
            end = getEnd().getName();
        } catch (JEVisException ex) {
        }
        return "JEVisClassRelationshipSQL{ type=" + _type + ", '" + start + "'-> '" + end + "}";
    }

    @Override
    public boolean isInHerited() throws JEVisException {
        return _isInHereted;
    }

    private JEVisClass replaceHerit(JEVisClass jclass) throws JEVisException {
//        System.out.println("find herit name for: " + jclass.getName());
        if (_isInHereted) {
            for (JEVisClass jc : heirList(new LinkedList<JEVisClass>(), _heir)) {
                if (jclass.getName().equals(jc.getName())) {
//                    System.out.println("found herit name: " + jc.getName() + "->" + _heir.getName());
                    return _heir;
                }
            }
        }

        return jclass;

    }

    private List<JEVisClass> heirList(List<JEVisClass> list, JEVisClass jclass) throws JEVisException {
        if (jclass.getInheritance() != null) {
            list.add(jclass.getInheritance());
            return heirList(list, jclass.getInheritance());
        } else {
//            System.out.print(_heir.getName());
//            for (JEVisClass jc : list) {
//                System.out.print("->" + jc.getName());
//            }
//            System.out.println("");
            return list;
        }
    }

    protected void setHeir(JEVisClass heir) {
        _isInHereted = true;
        _heir = heir;
    }
}
