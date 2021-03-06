/**
 * Copyright (C) 2009 - 2013 Envidatec GmbH <info@envidatec.com>
 *
 * This file is part of JEAPI.
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisDataSource;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisExceptionCodes;
import org.jevis.api.JEVisType;
import org.jevis.api.JEVisUnit;
import org.jevis.commons.unit.JEVisUnitImp;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisTypeSQL implements JEVisType {

    private JEVisDataSourceSQL _ds;
    private String _name;
    private String _guiType;
    private JEVisClass _class;
    private JEVisUnit _unit;
    private JEVisUnit _junit;
    private int _guiWeight;
    private int _premitivType;
    private int _validity;
    private String _description;
    private String _oldName;
    private boolean _hasChanged = false;
    private String cValue;
    private String _altSymbol;
    private String _unitString;

    public JEVisTypeSQL(JEVisDataSourceSQL ds, ResultSet rs, JEVisClass jclass) throws JEVisException {
        try {
            _ds = ds;
            _class = jclass;
            _name = rs.getString(TypeTable.COLUMN_NAME);
            _guiType = rs.getString(TypeTable.COLUMN_DISPLAY_TYPE);
            _guiWeight = rs.getInt(TypeTable.COLUMN_GUI_WEIGHT);
            _premitivType = rs.getInt(TypeTable.COLUMN_PRIMITIV_TYPE);
            _validity = rs.getInt(TypeTable.COLUMN_VALIDITY);

            _description = rs.getString(TypeTable.COLUMN_DESCRIPTION);
            cValue = rs.getString(TypeTable.COLUMN_VALUE);

            _altSymbol = rs.getString(TypeTable.COLUMN_ALT_SYMBOL);
            _unitString = rs.getString(TypeTable.COLUMN_DEFAULT_UNIT);

//            System.out.println("Unit in DB for " + _guiType + "\n  -> " + _unitString + " '" + _altSymbol + "'");
            //TODO unit imp
            if (_unitString != null && !_unitString.isEmpty()) {
                _unit = new JEVisUnitImp(_unitString, _altSymbol, JEVisUnit.Prefix.NANO);
            } else {
//                System.out.println("No unit in DB");
                _unit = new JEVisUnitImp("", "", JEVisUnit.Prefix.NANO);
            }

            //TODO implement unit
//            if (rs.getString(TypeTable.COLUMN_DEFAULT_UNIT) == null || rs.getString(TypeTable.COLUMN_DEFAULT_UNIT).isEmpty()) {
////                System.out.println("has no unit so we take Unit.ONE");
//                _junit = Unit.ONE;
//            } else {
//                System.out.println("Rase Unit String: '" + rs.getString(TypeTable.COLUMN_DEFAULT_UNIT) + "'");
//                _junit = Unit.valueOf(rs.getString(TypeTable.COLUMN_DEFAULT_UNIT));
////                _junit = UnitManager.getInstance().parseUnit(rs.getString(TypeTable.COLUMN_DEFAULT_UNIT));
//            }
        } catch (SQLException ex) {
//            ex.printStackTrace();
            throw new JEVisException("Cannot parse Object", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void setName(String name) {
//        System.out.println("setName: " + name);
        _hasChanged = true;
        _oldName = _name;
        _name = name;
    }

    @Override
    public int getPrimitiveType() {
        return _premitivType;
//        try {
//            if(_premitivType.equals("Double")){
//                return Double.class;
//            }else if(_premitivType.equals("Integer")){
//                return Integer.class;
//            }else if(_premitivType.equals("Enum/list")){//TODo implement this
//                return String.class;// String is the fallback
//            }
//            
//       } catch (Exception ex) {
//            System.out.println("Cannot found class: " + _premitivType);
//       }
//        return String.class;// String is the fallback
    }

    @Override
    public void setPrimitiveType(int type) {
//        System.out.println("setPrimitiveType: " + type);
        _hasChanged = true;
        _premitivType = type;
    }

    @Override
    public String getGUIDisplayType() {
        return _guiType;
    }

    @Override
    public void setGUIDisplayType(String type) {
//        System.out.println("setGUIDisplayType: " + type);
        _hasChanged = true;
        _guiType = type;
    }

    @Override
    public int getGUIPosition() {
        return _guiWeight;
    }

    @Override
    public void setGUIPosition(int pos) {
        _hasChanged = true;
        _guiWeight = pos;
    }

    @Override
    public JEVisClass getJEVisClass() {
        return _class;
    }

    @Override
    public int getValidity() {
        return _validity;
    }

    @Override
    public boolean delete() throws JEVisException {
        //TODO how to handel the userrigths for Classes and types?
        if (RelationsManagment.isSysAdmin(_ds.getCurrentUser())) {
            TypeTable tt = new TypeTable(_ds);

            return tt.detele(this);

        } else {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }

    }

    @Override
    public JEVisDataSource getDataSource() throws JEVisException {
        return _ds;
    }

    public String translate(Locale locale) {
        //TODO implement translate
        return getName();
    }

    @Override
    public void setValidity(int validity) {
        _hasChanged = true;
        _validity = validity;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 97 * hash + (this._name != null ? this._name.hashCode() : 0);
        hash = 97 * hash + (this._class != null ? this._class.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final JEVisTypeSQL other = (JEVisTypeSQL) obj;
        if ((this._name == null) ? (other._name != null) : !this._name.equals(other._name)) {
            return false;
        }
        if (this._class != other._class && (this._class == null || !this._class.equals(other._class))) {
            return false;
        }
        return true;
    }

    @Override
    public String getDescription() {
        return _description;
    }

    @Override
    public void setDescription(String description) {
        _hasChanged = true;
        _description = description;
    }

    @Override
    public String toString() {
        String className = "-";
        try {
            className = _class.getName();
        } catch (JEVisException ex) {
        }

        return "JEVisTypeSQL{" + "name=" + _name + ", guiType=" + _guiType + ", class=" + className + ", unit=" + _unit + ", guiWeight=" + _guiWeight + ", premitivType=" + _premitivType + ", validity=" + _validity + ", alternativSymbol=" + _altSymbol + '}';
    }

    @Override
    public void commit() throws JEVisException {
        if (!_hasChanged) {
            System.out.println("No changes, Abort");
            return;
        }
//        System.out.println("OldName: "+_oldName);
//        System.out.println("NewName: "+_name);

        TypeTable tt = new TypeTable(_ds);
        if (_oldName == null) {
//            System.out.println("update only");
            tt.update(this, getName());
        } else {
//            System.out.println("Name changed");
            tt.update(this, _oldName);
        }

    }

    @Override
    public void rollBack() {
//        _hasChanged=false;
        //TODO: implement rollBack

        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean hasChanged() {
        return _hasChanged;

    }

    @Override
    public JEVisUnit getUnit() throws JEVisException {
        return _junit;
    }

    @Override
    public void setUnit(JEVisUnit unit) {
        _hasChanged = true;
        _junit = unit;
    }

    @Override
    public String getConfigurationValue() {
        return cValue;
    }

    @Override
    public void setConfigurationValue(String value) {
        _hasChanged = true;
        cValue = value;
    }

    @Override
    public int compareTo(JEVisType compareObject) {
        try {
            if (getGUIPosition() < compareObject.getGUIPosition()) {
                return -1;
            } else if (getGUIPosition() == compareObject.getGUIPosition()) {
                return 0;
            } else {
                return 1;
            }
        } catch (JEVisException ex) {
            return 1;
        }
    }

    @Override
    public String getAlternativSymbol() throws JEVisException {

        if (_altSymbol == null) {
            _altSymbol = "";

        }
        return _altSymbol;
    }

    @Override
    public void setAlternativSymbol(String symbol) throws JEVisException {
        _hasChanged = true;
        _altSymbol = symbol;
        _hasChanged = true;
    }

}
