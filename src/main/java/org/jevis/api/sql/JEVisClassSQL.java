/**
 * Copyright (C) 2012 - 2015 Envidatec GmbH <info@envidatec.com>
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

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.logging.Level;
import javax.imageio.ImageIO;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisClassRelationship;
import org.jevis.api.JEVisConstants;
import org.jevis.api.JEVisDataSource;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisExceptionCodes;
import org.jevis.api.JEVisType;
import static org.jevis.api.JEVisConstants.*;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisClassSQL implements JEVisClass {

    public UUID idOne = UUID.randomUUID();

    private JEVisDataSourceSQL _ds;
    private BufferedImage _icon; //TODo is this a file resource?
    private String _discription;
    private String _name = "";
    private boolean isLoaded = false;
    private List<JEVisType> _types;
    private List<JEVisClassRelationship> _relations = new ArrayList<JEVisClassRelationship>();
    private String _oldName = null;
    private boolean _hasChanged = false;
    private boolean _unique;
    private JEVisClass _inheritance = null;
    private Logger logger = LoggerFactory.getLogger(JEVisClassSQL.class);
    private File _iconFile;
    private boolean _updateTypes = true;
    private DateTime _typeLastChanged = null;
    private boolean _relationshipLoaded = false;

    public JEVisClassSQL(JEVisDataSourceSQL ds, ResultSet rs) throws JEVisException {
        _ds = ds;
        //todo parsing
        isLoaded = true;

        try {
            _name = rs.getString(ClassTable.COLUMN_NAME);
//            System.out.println("JEVisClassSQL() UUID:" + idOne + "  name:" + _name);
            _oldName = _name;
            _discription = rs.getString(ClassTable.COLUMN_DESCRIPTION);

//            _inheritance = new JEVisClassSQL(ds, rs.getString(ClassTable.COLUMN_INHERIT));
            _unique = rs.getBoolean(ClassTable.COLUMN_UNIQUE);

            byte[] bytes = rs.getBytes(ClassTable.COLUMN_ICON);
            if (bytes != null && bytes.length > 0) {
                BufferedImage img = ImageIO.read(new ByteArrayInputStream(bytes));
//                _icon = new ImageIcon(img);
                _icon = img;
            }
            //TODO add group
        } catch (Exception ex) {

            ex.printStackTrace();
            throw new JEVisException("Cannot parse Class: " + _name, JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }

    }

    @Override
    public void setIcon(BufferedImage icon) {
//        System.out.println("Seticon: class: " + getName() + " UUID: " + idOne + " icon" + icon);
        _icon = icon;
        _hasChanged = true;
    }

    @Override
    public void setIcon(File icon) throws JEVisException {
        _iconFile = icon;
        try {
            _icon = ImageIO.read(icon);
//            System.out.println("set icon from file: " + _icon.getWidth());
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(JEVisClassSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
        _hasChanged = true;
    }

    protected File getFile() {
        return _iconFile;
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

    /**
     * Not the best way to secure that the class is leaded properly
     */
    private void load() {
//        if (!isLoaded) {
//            rollBack();
//        }
    }

    @Override
    public BufferedImage getIcon() {
        load();
//        System.out.println("getIcon: class: " + getName() + " UUID: " + idOne + "  icon:" + _icon);
        return _icon;
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
        getFixedRelationships();
        return _inheritance;
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

        try {
            final JEVisClass other = (JEVisClass) obj;

            return other.getName().equals(getName());
        } catch (JEVisException ex) {
            return false;
        }

//        final JEVisClassSQL other = (JEVisClassSQL) obj;
//        if ((this._name == null) ? (other._name != null) : !this._name.equals(other._name)) {
//            return false;
//        }
//        return true;
    }

    @Override
    public boolean isAllowedUnder(JEVisClass jevisClass) throws JEVisException {
        List<JEVisClass> vaild = getValidParents();
//        for (JEVisClass pClass : vaild) {
//            System.out.println("valid.parent: " + pClass);
//        }

        for (JEVisClass pClass : vaild) {
            if (pClass.getName().equals(jevisClass.getName())) {
                return true;
            }
        }
        return false;
    }

    public JEVisType getType(JEVisType type) throws JEVisException {
        for (JEVisType ty : getTypes()) {
            if (ty.equals(type)) {
                return ty;
            }
        }
        return null;
    }

    @Override
    public JEVisType getType(String typename) throws JEVisException {
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

        if (getInheritance() != null) {
            vaildParents.addAll(getInheritance().getValidParents());
        }

        for (JEVisClassRelationship rel : relations) {
            try {
                if (rel.isType(JEVisConstants.ClassRelationship.OK_PARENT)
                        && rel.getStart().equals(this)) {
                    if (!vaildParents.contains(rel.getOtherClass(this))) {
                        vaildParents.add(rel.getOtherClass(this));
                    }
                    vaildParents.addAll(rel.getOtherClass(this).getHeirs());

                }
            } catch (Exception ex) {
                logger.error("An JEClassRelationship had an error for '{}': {}", getName(), ex);
            }
        }

        Collections.sort(vaildParents);

        return vaildParents;
    }

    public DateTime getTypeLastChanged() {
        return _typeLastChanged;
    }

    @Override
    public List<JEVisType> getTypes() {
        if (_updateTypes || _types == null) {
            _typeLastChanged = DateTime.now();
//            System.out.println("Build type liste for: " + _name + " new date: " + _typeLastChanged + "  add: " + idOne);
            _updateTypes = false;
//            System.out.println("Type is null");
            try {
                TypeTable tt = new TypeTable(_ds);
                _types = tt.getAll(this);

                //TODo update hiers
                if (getInheritance() != null) {
                    _types.addAll(getInheritance().getTypes());
                }

//                System.out.println("size: " + _types.size());
                Collections.sort(_types);

            } catch (Exception ex) {
                System.out.println("error while getting Attributes: " + ex);
                ex.printStackTrace();
                return new ArrayList<JEVisType>();
            }
        }
//        System.out.println("getTypes()");

        return _types;
    }

    @Override
    public JEVisType buildType(String name) {
        //TODo check userrights
        try {
            TypeTable tt = new TypeTable(_ds);
            if (tt.insert(this, name)) {
//                _types = null;
                _updateTypes = true;
                for (JEVisType t : getTypes()) {
                    if (t.getName().equals(name)) {
//                        System.out.println("return new Type: " + t);

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
//        System.out.println("JEVisClass.commit()");
        if (!RelationsManagment.isSysAdmin(_ds.getCurrentUser())) {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }

        if (!_hasChanged) {
//            System.out.println("Nothing changed.. Abort ");
            return;
        }

        ClassTable cdb = _ds.getClassTable();
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
        List<JEVisClass> heirs = new LinkedList<JEVisClass>();
        for (JEVisClassRelationship cr : getRelationships(ClassRelationship.INHERIT, Direction.BACKWARD)) {
            try {
                heirs.add(cr.getStart());
                heirs.addAll(cr.getStart().getHeirs());
            } catch (Exception ex) {
                logger.warn("There is a problem with the Heir of {}", getName());
            }
        }
        return heirs;
    }

    @Override
    public boolean delete() throws JEVisException {
        if (RelationsManagment.isSysAdmin(_ds.getCurrentUser())) {
            return _ds.getClassTable().delete(this);
        } else {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }

    }

    private List<JEVisClassRelationship> getFixedRelationships() throws JEVisException {
//        if (_relations == null) {
        if (!_relationshipLoaded) {

            _relations = _ds.getClassRelationshipTable().get(this);

            for (JEVisClassRelationship crel : _relations) {
                try {
                    if (crel.isType(ClassRelationship.INHERIT) && crel.getStart().getName().equals(_name)) {
                        _inheritance = crel.getEnd();
                    }
                } catch (Exception ex) {
                    logger.info("There is a problem with an ClassRelationship for {}", this.getName());
//                    ex.printStackTrace();
                }
            }
            _relationshipLoaded = true;

        }

        return _relations;
    }

    @Override
    public List<JEVisClassRelationship> getRelationships() throws JEVisException {
        return getFixedRelationships();
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
//        System.out.println("buildRelationship: this:" + this.getName() + " to-class: " + jclass + "  type: " + type + "  direction: " + direction);
        JEVisClassRelationship newRel = null;
        if (direction == JEVisConstants.Direction.BACKWARD) {
            newRel = _ds.getClassRelationshipTable().insert(jclass, this, type);
        } else {
            newRel = _ds.getClassRelationshipTable().insert(this, jclass, type);
        }

        if (newRel != null && _relations != null) {
            _relations.add(newRel);
            try {
                //TODo: replace this workaround
                JEVisClassSQL otherClass = (JEVisClassSQL) jclass;
                otherClass._relations.add(newRel);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return newRel;

    }

    @Override
    public void deleteRelationship(JEVisClassRelationship rel) throws JEVisException {
        if (_ds.getClassRelationshipTable().delete(rel)) {
            _relations.remove(rel);
            //TODo: remove this workaround
            JEVisClassSQL otherClass = (JEVisClassSQL) rel.getOtherClass(this);
            otherClass._relations.remove(rel);
        }
    }

    @Override
    public int compareTo(JEVisClass o) {
        try {
            return getName().compareTo(o.getName());
        } catch (JEVisException ex) {
            return 1;
        }
    }
}
