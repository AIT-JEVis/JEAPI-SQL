/**
 * Copyright (C) 2009 - 2014 Envidatec GmbH <info@envidatec.com>
 *
 * This file is part of JEAPI-SQL.
 *
 * JAPI-SQL is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation in version 3.
 *
 * JAPI-SQL is distributed in the hope that it will be useful, but WITHOUT ANY
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

import java.util.List;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisObject;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisMultiSelectionSQL implements org.jevis.jeapi.JEVisMultiSelection {

    JEVisSampleSQL _sample;
    JEVisClass _class;
    JEVisObject _sObject;
    List<JEVisObject> _objects;
    List<JEVisObject> _selection;

    public JEVisMultiSelectionSQL(JEVisSampleSQL sample) {
        _sample = sample;
    }

    public JEVisClass getFilteredClass() throws JEVisException {
        if (_class == null) {
            System.out.println("getClass by name: " + _sample.getAttribute().getType().getConfigurationValue());
            _class = _sample.getDataSource().getJEVisClass(_sample.getAttribute().getType().getConfigurationValue());
        }

        if (_class == null) {
            System.out.println("Error no JEVisClass is set");
        }

        return _class;
    }

    public List<JEVisObject> getSelectableObjects() throws JEVisException {
        if (_objects == null) {
            _objects = _sample.getDataSource().getObjects(getFilteredClass(), true);
        }
        return _objects;
    }

    public List<JEVisObject> getSelectedObjects() throws JEVisException {
        if (_selection == null) {
            String vlaue = _sample.getValueAsString();
            String[] split = vlaue.split(";");

            for (int i = 0; i < split.length; i++) {
                try {
                    _selection.add(_sample.getDataSource().getObject(Long.parseLong(split[i])));
                } catch (Exception ex) {
                    //TODO: this can be a problem if a other user made a selection  
                    //      an one element is not accessable for the current usr.
                    System.out.println("invaild selection element: " + ex);
                }
            }
        }
        return _selection;
    }

    public void setSelectedObject(List<JEVisObject> objects) throws JEVisException {
        //TODO mabe a more save implementation
        String value = "";
        for (JEVisObject obj : objects) {
            if (getSelectableObjects().contains(obj)) {
                System.out.println("Valid selection");
                value += obj.getID() + ";";
            } else {
                System.out.println("Selecttion is not valid");
                //TODo throw error or so..
            }
        }
        _sample.setValue(value);

    }
}
