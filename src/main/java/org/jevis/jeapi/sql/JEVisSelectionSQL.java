/**
 * Copyright (C) 2009 - 2013 Envidatec GmbH <info@envidatec.com>
 *
 * This file is part of JEAPI.
 *
 * JEAPI-SQL is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software Foundation in version 3.
 *
 * JEAPI-SQL is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * JEAPI-SQL. If not, see <http://www.gnu.org/licenses/>.
 *
 * JEAPI-SQL is part of the OpenJEVis project, further project information are published
 * at <http://www.OpenJEVis.org/>.
 */
package org.jevis.jeapi.sql;

import java.util.List;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisObject;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisSelectionSQL implements org.jevis.jeapi.JEVisSelection{

    JEVisSampleSQL _sample;
    JEVisClass _class;
    JEVisObject _sObject;
    List<JEVisObject> _objects;
    
    
    public JEVisSelectionSQL(JEVisSampleSQL sample) {
        _sample= sample;
    }

    
    
    public JEVisClass getFilteredClass() throws JEVisException {
        
        if(_class==null){
            System.out.println("getClass by name: "+_sample.getAttribute().getType().getConfigurationValue());
           _class = _sample.getDataSource().getJEVisClass(_sample.getAttribute().getType().getConfigurationValue());
        }
        
        if(_class==null ){
            System.out.println("Error no JEVisClass is set");
        }
    
        return _class;
    }

    public List<JEVisObject> getSelectableObjects() throws JEVisException {
        if(_objects==null){
            _objects=_sample.getDataSource().getObjects(getFilteredClass(), true);
        }
        return _objects;
    }

    public JEVisObject getSelectedObject() throws JEVisException {
        if(_sObject==null){
            _sObject= _sample.getDataSource().getObject(_sample.getValueAsLong());
        }
        return _sObject;
    }

    public void setSelectedObject(JEVisObject object) throws JEVisException {
        if(getSelectableObjects().contains(object)){
            System.out.println("Valid selection");
            _sample.setValue(object.getID());
        }else{
            System.out.println("Selecttion is not valid");
            //TODo throw error or so..
        }
        
    }

    
    
}
