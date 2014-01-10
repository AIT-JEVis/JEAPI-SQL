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

import java.sql.ResultSet;
import java.sql.SQLException;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisObject;
import org.jevis.jeapi.JEVisRelationship;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisRelationshipSQL implements JEVisRelationship {

    private JEVisDataSourceSQL _ds;
    private long _start;
    private long _end;
    private JEVisObject _endObj = null;
    private JEVisObject _startObj = null;
    private JEVisObject[] _objects = new JEVisObject[2];
    private int _type;

    public JEVisRelationshipSQL(JEVisDataSourceSQL ds, long start, long end, int type) {
        this._ds = ds;
        this._start = start;
        this._end = end;
        this._type = type;
    }

    public JEVisRelationshipSQL(JEVisDataSourceSQL ds, ResultSet rs) throws SQLException {
        _ds = ds;
        _start = rs.getLong(RelationshipTable.COLUMN_START);
        _end = rs.getLong(RelationshipTable.COLUMN_END);
        _type = rs.getInt(RelationshipTable.COLUMN_TYPE);
    }

    public JEVisObject getStartObject() throws JEVisException {
        if (_startObj == null) {
            _startObj = _ds.getObjectTable().getObject(_start, true);
        }
        return _startObj;
    }

    public JEVisObject getEndObject() throws JEVisException {
        if (_endObj == null) {
            _endObj = _ds.getObjectTable().getObject(_end, true);
        }
        return _endObj;
    }

    public JEVisObject[] getObjects() throws JEVisException {
        _objects[0] = getStartObject();
        _objects[1] = getEndObject();
        return _objects;
    }

    public JEVisObject getOtherObject(JEVisObject object) throws JEVisException {
        //ToDo make it save
        if (object.getID() == getStartObject().getID()) {
            return getEndObject();
        } else if (object.getID() == getEndObject().getID()) {
            return getStartObject();
        } else {
            throw new JEVisException("Object is not part of this Relationship", 3423408);
        }
    }

    public int getType() throws JEVisException {
        return _type;
    }

    public boolean isType(int type) throws JEVisException {
        if (_type == type) {
            return true;
        } else {
            return false;
        }
    }

    public void delete() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String toString() {
        return "JEVisRelationshipImp{" + "start=" + _start + ", end=" + _end + ", type=" + _type + '}';
    }
}
