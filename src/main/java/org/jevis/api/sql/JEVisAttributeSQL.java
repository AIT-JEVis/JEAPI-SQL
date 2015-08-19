/**
 * Copyright (C) 2013 - 2013 Envidatec GmbH <info@envidatec.com>
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
package org.jevis.api.sql;

import com.google.gson.Gson;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.measure.unit.Unit;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisConstants;
import org.jevis.api.JEVisDataSource;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisExceptionCodes;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisSample;
import org.jevis.api.JEVisType;
import org.jevis.api.JEVisUnit;
import org.jevis.commons.dataprocessing.ProcessorObjectHandler;
import org.jevis.commons.dataprocessing.Task;
import org.jevis.commons.json.JsonUnit;
import org.jevis.commons.unit.JEVisUnitImp;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.ISOPeriodFormat;
import org.joda.time.format.PeriodFormatter;

/**
 *
 * @author Florian Simon<florian.simon@openjevis.org>
 */
public class JEVisAttributeSQL implements JEVisAttribute {

    //workaround remove
    List<JEVisSample> allSample;

    private String _name;
    private DateTime _minTS;
    private DateTime _maxTS;
    private long _objectID;
    private JEVisObject _object;
    private Period _period;
    private long _sampleCount;
    private JEVisDataSourceSQL _ds;
    private boolean _hasChanged = false;
//    private JEVisUnit _unit;
//    private String _unitString = "";
//    private String _altSymbol;

    private Period _inputRate = Period.ZERO;
    private Period _displayRate = Period.ZERO;
    private JEVisUnit _inputUnit = new JEVisUnitImp(Unit.ONE);
    private JEVisUnit _displayUnit = new JEVisUnitImp(Unit.ONE);

    public JEVisAttributeSQL(JEVisDataSourceSQL ds, ResultSet rs) throws JEVisException {
        try {

            _ds = ds;
            _name = rs.getString(AttributeTable.COLUMN_NAME);
            _sampleCount = rs.getLong(AttributeTable.COLUMN_COUNT);

            _objectID = rs.getLong(AttributeTable.COLUMN_OBJECT);
            _object = _ds.getObject(_objectID);

            _maxTS = new DateTime(rs.getTimestamp(AttributeTable.COLUMN_MAX_TS));
            if (rs.wasNull()) {
                _maxTS = null;
            }

            _minTS = new DateTime(rs.getTimestamp(AttributeTable.COLUMN_MIN_TS));
            if (rs.wasNull()) {
                _minTS = null;
            }

            PeriodFormatter format = ISOPeriodFormat.standard();
            if (rs.getString(AttributeTable.COLUMN_INPUT_RATE) != null && !rs.getString(AttributeTable.COLUMN_INPUT_RATE).isEmpty()) {
                _inputRate = format.parsePeriod(rs.getString(AttributeTable.COLUMN_INPUT_RATE));
            }
            if (rs.getString(AttributeTable.COLUMN_DISPLAY_RATE) != null && !rs.getString(AttributeTable.COLUMN_DISPLAY_RATE).isEmpty()) {
                _displayRate = format.parsePeriod(rs.getString(AttributeTable.COLUMN_DISPLAY_RATE));
            }

            if (rs.getString(AttributeTable.COLUMN_DISPLAY_UNIT) != null && !rs.getString(AttributeTable.COLUMN_DISPLAY_UNIT).isEmpty()) {
                try {
                    _displayUnit = new JEVisUnitImp(new Gson().fromJson(rs.getString(AttributeTable.COLUMN_DISPLAY_UNIT), JsonUnit.class));
                } catch (Exception ex) {
                    System.out.println("could not parse display unit because: " + ex);
                    System.out.println("UnitString: '" + rs.getString(AttributeTable.COLUMN_DISPLAY_UNIT) + "'");
                }
            }

            if (rs.getString(AttributeTable.COLUMN_INPUT_UNIT) != null && !rs.getString(AttributeTable.COLUMN_INPUT_UNIT).isEmpty()) {
                try {
                    _inputUnit = new JEVisUnitImp(new Gson().fromJson(rs.getString(AttributeTable.COLUMN_INPUT_UNIT), JsonUnit.class));
                } catch (Exception ex) {
                    System.out.println("could not parse input unit because: " + ex);
                    System.out.println("UnitString: '" + rs.getString(AttributeTable.COLUMN_INPUT_UNIT) + "'");
                }
            }

        } catch (SQLException ex) {
            throw new JEVisException("Cannot parse Object", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }
    }

    public JEVisAttributeSQL(JEVisDataSourceSQL ds, JEVisObject obj, JEVisType type) throws JEVisException {
        _name = type.getName();
        _object = obj;
        _ds = ds;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public boolean delete() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JEVisType getType() throws JEVisException {
        return getObject().getJEVisClass().getType(_name);
    }

    @Override
    public JEVisObject getObject() {
        return _object;
    }

    @Override
    public List<JEVisSample> getAllSamples() {
        try {
            SampleTable st = new SampleTable(_ds);
            List<JEVisSample> samples = st.getAll(this);

            return samples;
        } catch (Exception ex) {
            System.out.println("error while resiving samples: " + ex);
            return new ArrayList<JEVisSample>();
        }
    }

    @Override
    public List<JEVisSample> getSamples(DateTime from, DateTime to) {
        try {
            SampleTable st = new SampleTable(_ds);
            List<JEVisSample> samples = st.getSamples(this, from, to);
            return samples;
        } catch (Exception ex) {
            System.out.println("error while resiving samples: " + ex);
            return new ArrayList<JEVisSample>();
        }
    }

    @Override
    public int addSamples(List<JEVisSample> samples) throws JEVisException {
        //ToDo: check if sample are OK

//        System.out.println("sample to import");
//        for (JEVisSample sample : samples) {
//            System.out.println("sample: " + sample);
//        }
        if (!RelationsManagment.canWrite(_ds.getCurrentUser(), _object)) {
            throw new JEVisException("Insufficient user rights", 550);
        }

        SampleTable st = new SampleTable(_ds);
        int count = st.insertSamples(this, samples);
        System.out.println("imported " + count);

        //update count,min,max in attribute table
        //TODO: maybe its saver to query the acctual value from the DB i will take the fast way for now
        for (JEVisSample sample : samples) {
            if (sample instanceof JEVisSampleSQL) {
                if (_minTS == null || sample.getTimestamp().isBefore(getTimestampFromFirstSample())) {
                    _minTS = sample.getTimestamp();
                }
                if (_maxTS == null || sample.getTimestamp().isAfter(getTimestampFromLastSample())) {
                    _maxTS = sample.getTimestamp();
                }
                JEVisSampleSQL sqls = (JEVisSampleSQL) sample;
                sqls.setChanged(false);
                //TODO make this save
            }
        }

        commit();

        return count;//todo get real count

    }

    @Override
    public long getSampleCount() {
        return _sampleCount;
    }

    @Override
    public void rollBack() throws JEVisException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean hasChanged() {
        return _hasChanged = true;
    }

    @Override
    public void commit() throws JEVisException {
        _ds.getAttributeTable().updateAttributeTS(this);
        List<JEVisAttribute> atts = _ds.getAttributeTable().getAttributes(_object);
        for (JEVisAttribute att : atts) {
            if (att.getName().equals(getName())) {
                _sampleCount = att.getSampleCount();
                _minTS = att.getTimestampFromFirstSample();
                _maxTS = att.getTimestampFromLastSample();
            }
        }

    }

    @Override
    public JEVisSample getLatestSample() {
        try {
            SampleTable st = new SampleTable(_ds);
            JEVisSample sample = st.getLatest(this);
            return sample;
        } catch (NullPointerException ne) {
            return null;//TODO return NUll-Object?
        } catch (Exception ex) {
            System.out.println("error while resiving samples: " + ex);
            return null;//TODO return NUll-Object?
        }
    }

    @Override
    public int getPrimitiveType() throws JEVisException {
        return getType().getPrimitiveType();
    }

    @Override
    public boolean hasSample() {
        if (_sampleCount > 0) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public DateTime getTimestampFromFirstSample() {
        return _minTS;
    }

    @Override
    public DateTime getTimestampFromLastSample() {
        return _maxTS;
    }

    @Override
    public boolean deleteAllSample() throws JEVisException {
        if (RelationsManagment.canDelete(_ds.getCurrentUser(), _object)) {
            SampleTable st = new SampleTable(_ds);

            _minTS = null;
            _maxTS = null;
            _sampleCount = 0;

            commit();

            return st.deleteAllSamples(this);
        } else {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }

    }

    @Override
    public boolean deleteSamplesBetween(DateTime from, DateTime to) throws JEVisException {
        if (RelationsManagment.canDelete(_ds.getCurrentUser(), _object)) {
            SampleTable st = new SampleTable(_ds);
            //TODO: update samplecound.
            return st.deleteSamples(this, from, to);
        } else {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }
    }

    @Override
    public JEVisDataSource getDataSource() {
        return _ds;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + (this._name != null ? this._name.hashCode() : 0);
        hash = 47 * hash + (int) (this._objectID ^ (this._objectID >>> 32));
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
        final JEVisAttributeSQL other = (JEVisAttributeSQL) obj;
        if ((this._name == null) ? (other._name != null) : !this._name.equals(other._name)) {
            return false;
        }
        if (this._objectID != other._objectID) {
            return false;
        }
        return true;
    }

    @Override
    public boolean isType(JEVisType type) {
        try {
            if (type != null) {
                if (type.equals(getType())) {;
                    return true;
                }
            }
            return false;
        } catch (Exception ex) {
            return false;
        }

    }

    @Override
    public JEVisSample buildSample(DateTime ts, double value, JEVisUnit unit) throws JEVisException {
        return buildSample(ts, value, "", unit);
    }

    @Override
    public JEVisSample buildSample(DateTime ts, double value, String note, JEVisUnit unit) throws JEVisException {
        return buildSample(ts, getInputUnit().converteTo(unit, value), note);
    }

    //TODO will this replace the other
    @Override
    public JEVisSample buildSample(DateTime ts, Object value) throws JEVisException {
        return buildSample(ts, value, "");
    }

    @Override
    public JEVisSample buildSample(DateTime ts, Object value, String note) throws JEVisException {
//        System.out.println("build sample: " + ts + "  " + value + "   " + note);
        if (ts == null) {
            ts = new DateTime();
        }
        JEVisSample sample = new JEVisSampleSQL(_ds, this, value, ts);
        sample.setNote(note);
        if (_maxTS == null) {
            _maxTS = sample.getTimestamp();
            _minTS = sample.getTimestamp();
        } else if (sample.getTimestamp().isAfter(_maxTS)) {
            _maxTS = sample.getTimestamp();
        } else if (sample.getTimestamp().isBefore(_minTS)) {
            _minTS = sample.getTimestamp();
        }
//        sample.commit();

        return sample;
    }

    @Override
    public String toString() {
        String lastV = "-";

        if (_maxTS != null) {
            try {
                if (_maxTS != null) {
                    JEVisSample last = getLatestSample();
                    if (last.getAttribute().getPrimitiveType() == JEVisConstants.PrimitiveType.FILE) {
                        lastV = "[file:" + last.getValueAsFile().getFilename() + "]";
                    } else if (last.getAttribute().getPrimitiveType() == JEVisConstants.PrimitiveType.SELECTION) {
                        lastV = "[SelectedListObject:" + last.getValueAsSelection().getSelectedObject().getName() + "]";
                    } else {
                        lastV = getLatestSample().getValue().toString();
                    }
                }

            } catch (Exception ex) {
                lastV = "Error";
            }
        }

        return "JEVisAttributeSQL{" + "name=" + _name + ", lastValue=" + lastV
                + ", minTS=" + _minTS + ", maxTS=" + _maxTS + ", object="
                + _object.getID() + ", period=" + _period + ", sampleCount="
                + _sampleCount + '}';
    }

    @Override
    public int compareTo(JEVisAttribute compareObject) {
        try {

            if (getType().getGUIPosition() < compareObject.getType().getGUIPosition()) {
                return -1;
            } else if (getType().getGUIPosition() == compareObject.getType().getGUIPosition()) {
                return 0;
            } else {
                return 1;
            }
        } catch (Exception ex) {
            return 1;
        }
    }

    protected void increasedCount() {
        _sampleCount++;
    }

    @Override
    public JEVisUnit getDisplayUnit() throws JEVisException {
        return _displayUnit;
    }

    @Override
    public void setDisplayUnit(JEVisUnit unit) throws JEVisException {
        _hasChanged = true;
        _displayUnit = unit;
    }

    @Override
    public JEVisUnit getInputUnit() throws JEVisException {
        return _inputUnit;
    }

    @Override
    public void setInputUnit(JEVisUnit unit) throws JEVisException {
        _hasChanged = true;
        _inputUnit = unit;
    }

    @Override
    public Period getDisplaySampleRate() {
        return _displayRate;
    }

    @Override
    public Period getInputSampleRate() {
        return _inputRate;
    }

    @Override
    public void setInputSampleRate(Period period) {
        _hasChanged = true;
        _inputRate = period;
    }

    @Override
    public void setDisplaySampleRate(Period period) {
        _hasChanged = true;
        _displayRate = period;
    }

}
