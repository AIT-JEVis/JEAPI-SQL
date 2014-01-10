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
package org.jevis.jeapi.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.jevis.jeapi.JEVisAttribute;
import org.jevis.jeapi.JEVisConstants;
import org.jevis.jeapi.JEVisDataSource;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisExceptionCodes;
import org.jevis.jeapi.JEVisObject;
import org.jevis.jeapi.JEVisSample;
import org.jevis.jeapi.JEVisType;
import org.jevis.jeapi.JEVisUnit;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.joda.time.format.ISOPeriodFormat;
import org.joda.time.format.PeriodFormatter;

/**
 *
 * @author Florian Simon<florian.simon@openjevis.org>
 */
public class JEVisAttributeSQL implements JEVisAttribute {

    private String _name;
    private DateTime _minTS;
    private DateTime _maxTS;
    private long _objectID;
    private JEVisObject _object;
    private Period _period;
    private long _sampleCount;
    private JEVisDataSourceSQL _ds;

    public JEVisAttributeSQL(JEVisDataSourceSQL ds, ResultSet rs) throws JEVisException {
        try {
            _ds = ds;
            _name = rs.getString(AttributeTable.COLUMN_NAME);
            _sampleCount = rs.getLong(AttributeTable.COLUMN_COUNT);

            _objectID = rs.getLong(AttributeTable.COLUMN_OBJECT);

//            System.out.println("MaxTS: "+rs.getString(AttributeTable.COLUMN_MAX_TS));
            _maxTS = new DateTime(rs.getTimestamp(AttributeTable.COLUMN_MAX_TS));
//            System.out.println("MaxTs.dt: "+_maxTS);
            _minTS = new DateTime(rs.getTimestamp(AttributeTable.COLUMN_MIN_TS));
            _object = _ds.getObject(_objectID);

            if (rs.getString(AttributeTable.COLUMN_PERIOD) != null && !rs.getString(AttributeTable.COLUMN_PERIOD).isEmpty()) {

                PeriodFormatter format = ISOPeriodFormat.standard();
                _period = format.parsePeriod(rs.getString(AttributeTable.COLUMN_PERIOD));
            }


            //TODO add group
        } catch (SQLException ex) {
            throw new JEVisException("Cannot parse Object", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }
    }

    public JEVisAttributeSQL(JEVisDataSourceSQL ds, JEVisObject obj, JEVisType type) throws JEVisException {
        _name = type.getName();
        _object = obj;
        _ds = ds;
    }

    public String getName() {
        return _name;
    }

    public boolean delete() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public JEVisType getType() throws JEVisException {
        return getObject().getJEVisClass().getType(_name);
    }

    public JEVisObject getObject() {
        return _object;
    }

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

    public int addSamples(List<JEVisSample> samples) throws JEVisException {
        //ToDo: check if sample are OK

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
            }
        }

        _sampleCount = _sampleCount + count;
        commit();

        return count;//todo get real count

    }

    public long getSampleCount() {
        return _sampleCount;
    }

    private void commit() throws JEVisException {
        AttributeTable at = new AttributeTable(_ds);
        at.updateAttributeTS(this);
    }

    public JEVisSample getLatestSample() {
        try {
            SampleTable st = new SampleTable(_ds);
            JEVisSample sample = st.getLatest(this);
            return sample;
        } catch (Exception ex) {
            System.out.println("error while resiving samples: " + ex);
            return null;//TODO return emty Sample?
        }
    }

    public int getPrimitiveType() throws JEVisException {
        return getType().getPrimitiveType();
    }

    public boolean hasSample() {
        if (_sampleCount > 0) {
            return true;
        } else {
            return false;
        }
    }

    public DateTime getTimestampFromFirstSample() {
        return _minTS;
    }

    public DateTime getTimestampFromLastSample() {
        return _maxTS;
    }

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

    public boolean deleteSamplesBetween(DateTime from, DateTime to) throws JEVisException {
        if (RelationsManagment.canDelete(_ds.getCurrentUser(), _object)) {
            SampleTable st = new SampleTable(_ds);
            return st.deleteSamples(this, from, to);
        } else {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }
    }

    public JEVisUnit getUnit() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void setUnit(JEVisUnit unit) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public JEVisDataSource getDataSource() {
        return _ds;
    }

    public Period getPeriod() {
        return _period;
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

    //TODO will this replace the other 
    public JEVisSample buildSample(DateTime ts, Object value) throws JEVisException {
        return buildSample(ts, value, "");
    }

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
}
