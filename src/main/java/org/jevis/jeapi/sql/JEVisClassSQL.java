/**
 * Copyright (C) 2009 - 2013 Envidatec GmbH <info@envidatec.com>
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

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisClassRelationship;
import org.jevis.jeapi.JEVisConstants;
import org.jevis.jeapi.JEVisDataSource;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisExceptionCodes;
import org.jevis.jeapi.JEVisRelationship;
import org.jevis.jeapi.JEVisType;
import static org.jevis.jeapi.JEVisConstants.*;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisClassSQL implements JEVisClass {

    private JEVisDataSourceSQL _ds;
    private ImageIcon _icon; //TODo is this a file resource?
    private String _discription;
    private String _name = "";
    private boolean isLoaded = false;
    private List<JEVisType> _types;
    private List<JEVisClassRelationship> _relations;
    private String _oldName = null;
    private boolean _hasChanged = false;
    private boolean _unique;

    public JEVisClassSQL(JEVisDataSourceSQL ds, ResultSet rs) throws JEVisException {
        _ds = ds;
        //todo parsing
        isLoaded = true;

        try {
            _name = rs.getString(ClassTable.COLUMN_NAME);
            _oldName = _name;
            _discription = rs.getString(ClassTable.COLUMN_DESCRIPTION);

//            _inheritance = new JEVisClassSQL(ds, rs.getString(ClassTable.COLUMN_INHERIT));
            _unique = rs.getBoolean(ClassTable.COLUMN_UNIQUE);


            byte[] bytes = rs.getBytes(ClassTable.COLUMN_ICON);
            if (bytes != null && bytes.length > 0) {
                BufferedImage img = ImageIO.read(new ByteArrayInputStream(bytes));
                _icon = new ImageIcon(img);
            }



            //TODO add group
        } catch (Exception ex) {
            throw new JEVisException("Cannot parse Class: " + _name, JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }



    }

    public JEVisClassSQL(JEVisDataSourceSQL ds, String name) {
        _ds = ds;
        _name = name;
        isLoaded = false;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void setName(String name) {
//        _oldName= _name;
        _name = name;
        _hasChanged = true;
    }

    public ImageIcon getIcon() {
        return _icon;
    }

    @Override
    public void setIcon(ImageIcon icon) {
        _icon = icon;
        _hasChanged = true;
    }

    @Override
    public String getDescription() {
        return _discription;
    }

    @Override
    public void setDescription(String discription) {
        _discription = discription;
        _hasChanged = true;
    }

    @Override
    public JEVisClass getInheritance() throws JEVisException {
        for (JEVisClassRelationship rel : getRelationships(ClassRelationship.INHERIT, Direction.FORWARD)) {
            return rel.getStart();
        }
        return null;
    }

    @Override
    public JEVisDataSource getDataSource() throws JEVisException {
        return _ds;
    }

    public String translate(Locale locale) {
        System.out.println("translate Not supported yet.");
        return getName();
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 43 * hash + (this._name != null ? this._name.hashCode() : 0);
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
        final JEVisClassSQL other = (JEVisClassSQL) obj;
        if ((this._name == null) ? (other._name != null) : !this._name.equals(other._name)) {
            return false;
        }
        return true;
    }

    @Override
    public boolean isAllowedUnder(JEVisClass jevisClass) throws JEVisException {
        System.out.println("isAllowedUnder: " + jevisClass);
        List<JEVisClass> vaild = getValidParents();
        for (JEVisClass pClass : vaild) {
            System.out.println("pClass: " + pClass);
        }

        for (JEVisClass pClass : vaild) {
            System.out.println("compare: " + pClass);
            if (pClass.getName().equals(jevisClass.getName())) {
                return true;
            }
        }



        return false;
    }

    @Override
    public List<JEVisType> getTypes() {
        if (_types == null) { //Cach is disabled for the mean time. TODO iake a good cach implementation
//        if (true) {
            try {
                TypeTable tt = new TypeTable(_ds);
                _types = tt.getAll(this);

            } catch (Exception ex) {
                System.out.println("error while getting Attributes: " + ex);
                return new ArrayList<JEVisType>();
            }

        }

        return _types;
    }

    public JEVisType getType(JEVisType type) {
        for (JEVisType ty : getTypes()) {
            if (ty.equals(type)) {
                return ty;
            }
        }
        return null;
    }

    @Override
    public JEVisType getType(String typename) {
        for (JEVisType ty : getTypes()) {
            if (ty.getName().equals(typename)) {
                return ty;
            }
        }
        return null;
    }

    @Override
    public List<JEVisClass> getValidParents() throws JEVisException {
        List<JEVisClass> vaildParents = new LinkedList<JEVisClass>();
        List<JEVisClassRelationship> relations = getRelationships();


        for (JEVisClassRelationship rel : relations) {
            if (rel.isType(JEVisConstants.ClassRelationship.OK_PARENT)) {
                System.out.println("addValidParent: " + rel);
                vaildParents.add(rel.getOtherClass(this));
            }
        }


        if (getInheritance() != null) {
            vaildParents.addAll(getInheritance().getValidParents());
        }

        return vaildParents;
    }

    @Override
    public JEVisType buildType(String name) {
        //TODo check userrights
        try {
            TypeTable tt = new TypeTable(_ds);
            if (tt.insert(this, name)) {
                _types = null;
                for (JEVisType t : getTypes()) {
                    if (t.getName().equals(name)) {
                        return t;
                    }
                }
            }
            return null;

        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }


    }

    @Override
    public void commit() throws JEVisException {
        System.out.println("JEVisClass.commi()");
        if (!RelationsManagment.isSysAdmin(_ds.getCurrentUser())) {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }


        if (!_hasChanged) {
            System.out.println("Nothing changed.. Abort ");
            return;
        }

        ClassTable cdb = new ClassTable(_ds);
        cdb.update(this, _oldName);


        _hasChanged = false;
    }

    @Override
    public boolean hasChanged() {
        return _hasChanged;
    }

    @Override
    public void rollBack() {
        try {
            JEVisClass original = _ds.getJEVisClass(_name);
            _name = original.getName();
            _discription = original.getDescription();

            _icon = original.getIcon();

            _hasChanged = false;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public boolean isUnique() {
        return _unique;
    }

    @Override
    public void setUnique(boolean unique) {
        _hasChanged = true;
        _unique = unique;
    }

    @Override
    public String toString() {
        String inherit = "null";
        String discription = "null";
        try {
            if (getInheritance() != null) {
                inherit = getInheritance().getName();
            }
            if (_discription != null) {
                discription = _discription;
            }
        } catch (JEVisException ex) {
        }
        return "JEVisClassSQL{" + " name=" + _name + ",discription=" + discription + ", inheritance=" + inherit + ", unique=" + _unique + '}';
    }

    @Override
    public List<JEVisClass> getHeirs() throws JEVisException {
        ClassTable ct = new ClassTable(_ds);
        return ct.getAllHeirs(this);
    }

    @Override
    public boolean delete() throws JEVisException {
        if (RelationsManagment.isSysAdmin(_ds.getCurrentUser())) {
            return _ds.getClassTable().delete(this);
        } else {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }

    }

    @Override
    public List<JEVisClassRelationship> getRelationships() throws JEVisException {
        if (_relations == null) {
            _relations = _ds.getClassRelationshipTable().get(this);
        }
        return _relations;
    }

    @Override
    public List<JEVisClassRelationship> getRelationships(int type) throws JEVisException {
        List<JEVisClassRelationship> tmp = new LinkedList<JEVisClassRelationship>();

        for (JEVisClassRelationship cr : getRelationships()) {
            if (cr.isType(type)) {
                tmp.add(cr);
            }
        }

        return tmp;
    }

    @Override
    public List<JEVisClassRelationship> getRelationships(int type, int direction) throws JEVisException {
        List<JEVisClassRelationship> tmp = new LinkedList<JEVisClassRelationship>();

        for (JEVisClassRelationship cr : getRelationships(type)) {
            if (direction == JEVisConstants.Direction.FORWARD && cr.getStart().equals(this)) {
                tmp.add(cr);
            } else if (direction == JEVisConstants.Direction.BACKWARD && cr.getEnd().equals(this)) {
                tmp.add(cr);
            }
        }

        return tmp;
    }

    @Override
    public JEVisClassRelationship buildRelationship(JEVisClass jclass, int type, int direction) throws JEVisException {
        JEVisClassRelationship newRel = null;
        if (direction == JEVisConstants.Direction.FORWARD) {
            newRel = _ds.getClassRelationshipTable().insert(jclass, this, type);
        } else {
            newRel = _ds.getClassRelationshipTable().insert(this, jclass, type);
        }
        if (newRel != null) {
            _relations.add(newRel);
        }
        return newRel;

    }

    @Override
    public void deleteRelationship(JEVisClassRelationship rel) throws JEVisException {
        if (_ds.getClassRelationshipTable().delete(rel)) {
            _relations.remove(rel);
        }
    }
}
