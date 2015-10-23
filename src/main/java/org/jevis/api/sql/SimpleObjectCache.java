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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.jevis.api.JEVisObject;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class SimpleObjectCache {

    public UUID idOne = UUID.randomUUID();
    private static SimpleObjectCache instance;
    private static Map<Long, JEVisObject> _cach;

    public SimpleObjectCache() {
//        _cach = new HashMap<Long, JEVisObject>();
        _cach = new ConcurrentHashMap<Long, JEVisObject>();
    }

    public static SimpleObjectCache getInstance() {
        if (SimpleObjectCache.instance == null) {
            SimpleObjectCache.instance = new SimpleObjectCache();
        }
        return SimpleObjectCache.instance;
    }

    public void addObject(JEVisObject object) {
//        System.out.println("new Chache: " + idOne + "   " + jclass.getName());
        _cach.put(object.getID(), object);
    }

    public JEVisObject getObject(long id) {
        return _cach.get(id);
    }

    public boolean contains(Long id) {
//        System.out.println("Cache?: " + name + " " + _cach.containsKey(name));
//        if (name.equals("System")) {
//            System.out.println("hier");
//        }

        return _cach.containsKey(id);
    }

    public void remove(Long id) {
        _cach.remove(id);
    }
}
