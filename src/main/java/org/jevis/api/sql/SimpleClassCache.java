/**
 * Copyright (C) 2015 Envidatec GmbH <info@envidatec.com>
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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.jevis.api.JEVisClass;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class SimpleClassCache {

    public UUID idOne = UUID.randomUUID();
    private static SimpleClassCache instance;
    private static Map<String, JEVisClass> _cach;

    public SimpleClassCache() {
        _cach = new HashMap<String, JEVisClass>();
    }

    public static SimpleClassCache getInstance() {
        if (SimpleClassCache.instance == null) {
            SimpleClassCache.instance = new SimpleClassCache();
        }
        return SimpleClassCache.instance;
    }

    public void addClass(JEVisClassSQL jclass) {
//        System.out.println("new Chache: " + idOne + "   " + jclass.getName());
        _cach.put(jclass.getName(), jclass);
    }

    public JEVisClass getJEVisClass(String name) {
        return _cach.get(name);
    }

    public boolean contains(String name) {
//        System.out.println("Cache?: " + name + " " + _cach.containsKey(name));
//        if (name.equals("System")) {
//            System.out.println("hier");
//        }

        return _cach.containsKey(name);
    }

    public void remove(String name) {
        _cach.remove(name);
    }

}
