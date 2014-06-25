/**
 * Copyright (C) 2009 - 2014 Envidatec GmbH <info@envidatec.com>
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

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.measure.converter.UnitConverter;
import javax.measure.unit.Unit;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisConstants;
import org.jevis.api.JEVisDataSource;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisExceptionCodes;
import org.jevis.api.JEVisFile;
import org.jevis.api.JEVisSelection;
import org.jevis.api.JEVisMultiSelection;
import org.jevis.api.JEVisSample;
import org.jevis.api.JEVisUnit;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Florian Simon<florian.simon@openjevis.org>
 */
public class JEVisSampleSQL implements JEVisSample {

    private String _attribute;
    private JEVisAttribute _jAttribute;
    private JEVisDataSourceSQL _ds;
    private DateTime _ts;
//    private String _value;
    private Object _tvalue;
    private String _note;
    private String _manip;
//    private JEVisManipulation _jManIP;
    private JEVisFile _file;
    private JEVisSelection _selection;
    private JEVisMultiSelection _mSelection;
    private boolean _hasChanged = false;
    private String _filename;
    private byte[] _fileBytes;
    private Logger logger = LoggerFactory.getLogger(JEVisSampleSQL.class);
    private Unit _unit = Unit.ONE;

    public JEVisSampleSQL(JEVisDataSourceSQL ds, JEVisAttribute att, Object value, DateTime ts) throws JEVisException {
        _ds = ds;
        _jAttribute = att;
        _tvalue = value;
        _ts = ts;

        if (value != null) {
            if (value instanceof String) {
//                System.out.println("sample is a String");
            } else if (value instanceof Double) {
//                System.out.println("sample is a Double");
            } else if (value instanceof Integer) {
//                System.out.println("sample is a Integer");
            } else if (value instanceof Boolean) {
//                System.out.println("sample is a Integer");            
            } else if (value instanceof File) {
//                System.out.println("sample is a file");
                _file = new JEVisFileSQL(this);
                try {
                    _file.loadFromFile((File) value);
                } catch (IOException ex) {
                    logger.error("Cannot load file fom Sample: {}", ex);
                }

            } else if (value instanceof JEVisFile) {
//                System.out.println("sample is a file");
                _file = new JEVisFileSQL(this);
                _file = (JEVisFile) value;

            } else if (value instanceof JEVisSelection) {
//                System.out.println("sample is a Selection");
            } else if (value instanceof JEVisMultiSelection) {
//                System.out.println("sample is a MulitSelection");
            }
        }

    }

    public JEVisSampleSQL(JEVisDataSourceSQL ds, ResultSet rs, JEVisAttribute att) throws JEVisException {
        try {
            _ds = ds;
            _attribute = rs.getString(SampleTable.COLUMN_ATTRIBUTE);
            _jAttribute = att;
            Timestamp ts = rs.getTimestamp(SampleTable.COLUMN_TIMESTAMP);
            _ts = new DateTime(ts);

            _note = rs.getString(SampleTable.COLUMN_NOTE);
            _manip = rs.getString(SampleTable.COLUMN_MANID);

//            _jManIP = new JEVisManipulationSQL(_manip);
            if (att.getPrimitiveType() == JEVisConstants.PrimitiveType.BOOLEAN) {
                _tvalue = rs.getBoolean(SampleTable.COLUMN_VALUE);
            } else {
                _tvalue = rs.getString(SampleTable.COLUMN_VALUE);
            }

            if (rs.getBytes(SampleTable.COLUMN_FILE) != null) {
                _fileBytes = rs.getBytes(SampleTable.COLUMN_FILE);
                _tvalue = _fileBytes;
                _filename = rs.getString(SampleTable.COLUMN_FILE_NAME);
            }

        } catch (SQLException ex) {
            throw new JEVisException("Cannot parse Object", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }

    }

    public String getFilename() {
        return _filename;
    }

    public void SetFilename(String fname) {
        _filename = fname;
        _hasChanged = true;
    }

    @Override
    public DateTime getTimestamp() {
        return _ts;
    }

    @Override
    public Object getValue() {
        return _tvalue;
    }

    //TODP better error handling
    @Override
    public String getValueAsString() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.STRING) {
                logger.warn("Warning the primitive type of this Type is not String");
            }
            return _tvalue.toString();

        } catch (Exception ex) {
            ex.printStackTrace();
            return "";
        }
    }

    @Override
    public Long getValueAsLong() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.LONG
                    && getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.SELECTION) {
                logger.warn("Warning the primitive type of this Type is not Long or Dynamic List");
            }
            return Long.parseLong((String) _tvalue);

        } catch (Exception ex) {
            ex.printStackTrace();
//            return -1l;
            throw new NumberFormatException();
        }
    }

    @Override
    public Long getValueAsLong(Unit unit) throws JEVisException {
        Number convNumber = getUnit().getConverterTo(unit).convert(getValueAsLong());
        return convNumber.longValue();

    }

    @Override
    public Double getValueAsDouble(Unit unit) throws JEVisException {
        Double value = getValueAsDouble();
        return unit.getConverterTo(getUnit()).convert(value);
    }

    @Override
    public Unit getUnit() throws JEVisException {
        return getAttribute().getUnit();
    }

    @Override
    public Double getValueAsDouble() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.DOUBLE) {
                logger.warn("Warning the primitive type of this Type is not Double");
                return Double.NaN;
            }
            return Double.parseDouble((String) _tvalue);

        } catch (Exception ex) {
            ex.printStackTrace();
            return Double.NaN;
        }
    }

    @Override
    public Boolean getValueAsBoolean() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.BOOLEAN) {
                logger.warn("Waring the primitive type of this Type is not Boolean");
                return null;
            }

            //DOTO: the DB give a string ? 
            if (_tvalue instanceof String) {
                if (((String) _tvalue).equals("1")) {
                    return true;
                } else {
                    return false;
                }
            } else {
                return (Boolean) _tvalue;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }

    }

//    public byte[] valueToByte() {
//         try{        
//            if(getAttribute().getPrimitiveType()!=JEVisConstants.GENERIC_TYPE_FILE){
//                 System.out.println("Error the primitive type of this Type is not byte");
//                 return null;
//            }
//            
//            return (byte[])_tvalue;
//        
//        }catch(Exception ex){
//            ex.printStackTrace();
//            return null;
//        }
//    }
    @Override
    public JEVisFile getValueAsFile() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.FILE) {
                logger.warn("Error the primitive type of this Type is not byte");
                return null;
            }

            if (_file != null) {
                return _file;
            } else {
                _file = new JEVisFileSQL(this);
            }

            return _file;

        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public JEVisSelection getValueAsSelection() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.SELECTION) {
                logger.warn("Error the primitive type of this Type is not selection");
                return null;
            }

            if (_selection != null) {
                return _selection;
            } else {
                _selection = new JEVisSelectionSQL(this);
            }

            return _selection;

        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public JEVisMultiSelection getValueAsMultiSelection() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.SELECTION) {
                logger.warn("Error the primitive type of this Type is not MultiSelection ");
                return null;
            }

            if (_mSelection != null) {
                return _mSelection;
            } else {
                _mSelection = new JEVisMultiSelectionSQL(this);
            }

            return _mSelection;

        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public void setValue(Object value) throws ClassCastException {
        _tvalue = value;
        _hasChanged = true;
    }

    @Override
    public void setValue(Object value, Unit unit) throws JEVisException {
        if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.DOUBLE) {
            _tvalue = unit.getConverterTo(getUnit()).convert((Double) value);
        } else if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.LONG) {
            Number tmp = unit.getConverterTo(getUnit()).convert((Long) value);
            _tvalue = tmp.longValue();
        } else {
            _tvalue = value;
            logger.warn("Error the primitive type of this Type is not MultiSelection ");
        }
        _hasChanged = true;

    }

    @Override
    public JEVisAttribute getAttribute() {
        return _jAttribute;
    }

    @Override
    public String getNote() {
        return _note;
    }

    @Override
    public void setNote(String note) {
        _hasChanged = true;
        _note = note;
    }

    @Override
    public String toString() {
        String val = "-";
        try {
            if (getAttribute().getPrimitiveType() == JEVisConstants.PrimitiveType.FILE) {
                val = "[file:" + getValueAsFile().getFilename() + "]";
            } else if (getAttribute().getPrimitiveType() == JEVisConstants.PrimitiveType.SELECTION) {
                val = "[SelectedListObject:" + getValueAsSelection().getSelectedObject().getName() + "]";
            } else {
                val = getValueAsString();
            }
        } catch (Exception ex) {
            val = "Error";
        }

        return "JEVisSampleImp{" + "ts=" + _ts + ", value=" + val + ", note=" + _note + " '}'";
    }

    @Override
    public JEVisDataSource getDataSource() throws JEVisException {
        return _ds;
    }

    @Override
    public void commit() throws JEVisException {
        if (!_hasChanged) {
            System.out.println("Nothing changed.. Abort ");
            return;
        }
        System.out.println("commiting changes");

        //wo do not use the direct way to cnadel the sample count
        List<JEVisSample> samples = new LinkedList<JEVisSample>();
        samples.add(this);
        getAttribute().addSamples(samples);
        _hasChanged = false;

//        //TODO: use an global SampleTable object so the not every sample need to create one->performace/memory
//        SampleTable sb = new SampleTable(_ds);
//
//        //TODO: maybe implement an Update funktion but for now we delete and the insert the sample 
////        sb.deleteSamples(_jAttribute, _ts, _ts);
//        List samples = new ArrayList(); //TODO: maybe we need a new funktion to import without list
//        samples.add(this);
//        sb.insertSamples(_jAttribute, samples);
//
    }

    @Override
    public void rollBack() throws JEVisException {
        SampleTable sb = new SampleTable(_ds);
        List<JEVisSample> js = sb.getSamples(_jAttribute, _ts, _ts);
        _tvalue = js.get(0).getValue();
//        _jManIP=js.get(0).getManipulation();
        _note = js.get(0).getNote();
        _file = null;
        _hasChanged = false;
    }

    protected void setChanged(boolean hasChanged) {
        _hasChanged = false;
    }

    @Override
    public boolean hasChanged() {
        return _hasChanged;
    }
//    public JEVisManipulation getManipulation() {
//        if(_jManIP==null){
//            if(_manip==null){
//                _jManIP = new JEVisManipulationSQL("");                
//            }else{
//                _jManIP = new JEVisManipulationSQL(_manip);     
//            }
//        }
//        
//        return _jManIP;
//    }
}