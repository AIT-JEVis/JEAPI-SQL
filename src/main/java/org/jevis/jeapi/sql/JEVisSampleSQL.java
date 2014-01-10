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
package org.jevis.jeapi.sql;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jevis.jeapi.JEVisAttribute;
import org.jevis.jeapi.JEVisConstants;
import org.jevis.jeapi.JEVisDataSource;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisExceptionCodes;
import org.jevis.jeapi.JEVisFile;
import org.jevis.jeapi.JEVisSelection;
import org.jevis.jeapi.JEVisMultiSelection;
import org.jevis.jeapi.JEVisSample;
import org.joda.time.DateTime;

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
            } else if (value instanceof File) {
//                System.out.println("sample is a file");
                _file = new JEVisFileSQL(this);
                try {
                    _file.loadFromFile((File) value);
                } catch (IOException ex) {
                    Logger.getLogger(JEVisSampleSQL.class.getName()).log(Level.SEVERE, null, ex);
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

    public DateTime getTimestamp() {
        return _ts;
    }

    public Object getValue() {
        return _tvalue;
    }

    //TODP better error handling
    public String getValueAsString() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.STRING) {
                System.out.println("Warning the primitive type of this Type is not String");
            }
            return _tvalue.toString();

        } catch (Exception ex) {
            ex.printStackTrace();
            return "";
        }
    }

    public Long getValueAsLong() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.LONG
                    && getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.SELECTION) {
                System.out.println("Warning the primitive type of this Type is not Long or Dynamic List");
            }
            return Long.parseLong((String) _tvalue);

        } catch (Exception ex) {
            ex.printStackTrace();
            return -1l;
        }
    }

    public Double getValueAsDouble() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.DOUBLE) {
                System.out.println("Warning the primitive type of this Type is not Double");
                return Double.NaN;
            }
            return Double.parseDouble((String) _tvalue);

        } catch (Exception ex) {
            ex.printStackTrace();
            return Double.NaN;
        }
    }

    /**
     * {@inheritDoc}
     */
    public Boolean getValueAsBoolean() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.BOOLEAN) {
                System.out.println("Error the primitive type of this Type is not Boolean");
                return null;
            }
            return Boolean.parseBoolean((String) _tvalue);

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
    public JEVisFile getValueAsFile() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.FILE) {
                System.out.println("Error the primitive type of this Type is not byte");
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

    public JEVisSelection getValueAsSelection() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.SELECTION) {
                System.out.println("Error the primitive type of this Type is not selection");
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

    public JEVisMultiSelection getValueAsMultiSelection() {
        try {
            if (getAttribute().getPrimitiveType() != JEVisConstants.PrimitiveType.SELECTION) {
                System.out.println("Error the primitive type of this Type is not MultiSelection ");
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

    public void setValue(Object value) throws ClassCastException {
        _tvalue = value;
        _hasChanged = true;
    }

    public JEVisAttribute getAttribute() {
        return _jAttribute;
    }

    public String getNote() {
        return _note;
    }

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

    public JEVisDataSource getDataSource() throws JEVisException {
        return _ds;
    }

    public void commit() throws JEVisException {
        if (!_hasChanged) {
            System.out.println("Nothing changed.. Abort ");
            return;
        }
        System.out.println("commiting changes");



        //TODO: use an global SampleTable object so the not every sample need to create one->performace/memory
        SampleTable sb = new SampleTable(_ds);

        //TODO: maybe implement an Update funktion but for now we delete and the insert the sample 
//        sb.deleteSamples(_jAttribute, _ts, _ts);
        List samples = new ArrayList(); //TODO: maybe we need a new funktion to import without list
        samples.add(this);
        sb.insertSamples(_jAttribute, samples);

    }

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
