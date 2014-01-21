/**
 * Copyright (C) 2009 - 2014 Envidatec GmbH <info@envidatec.com>
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.jevis.jeapi.JEVisAttribute;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisClassRelationship;
import org.jevis.jeapi.JEVisConstants;
import org.jevis.jeapi.JEVisDataSource;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisExceptionCodes;
import org.jevis.jeapi.JEVisObject;
import org.jevis.jeapi.JEVisRelationship;
import org.jevis.jeapi.JEVisType;

import static org.jevis.jeapi.sql.ObjectTable.COLUMN_ID;
import static org.jevis.jeapi.sql.ObjectTable.COLUMN_LINK;
import static org.jevis.jeapi.sql.ObjectTable.COLUMN_NAME;
import static org.jevis.jeapi.sql.ObjectTable.COLUMN_PARENT;
import static org.jevis.jeapi.sql.ObjectTable.COLUMN_CLASS;
import static org.jevis.jeapi.sql.ObjectTable.COLUMN_GROUP;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Florian Simon<florian.simon@openjevis.org>
 */
public class JEVisObjectSQL implements JEVisObject {

    private boolean _hasChanged = false;
    private String _name;
    private long _id;
    private long _link;
    private long _parentID;
    private List<JEVisObject> _parentObjs;
    private String _class;
    private long _groupID;
    private JEVisDataSourceSQL _ds;
    private List<JEVisObject> _childrenObj;
    private JEVisClass _classObj;
    private List<JEVisAttribute> _attributes;
    private List<JEVisRelationship> _relationships = new LinkedList<JEVisRelationship>();
    private boolean _isLink = false;
    private JEVisObject _linkedObject = null;
    private Logger logger = LoggerFactory.getLogger(JEVisDataSourceSQL.class);

    public JEVisObjectSQL(JEVisDataSourceSQL ds, ResultSet rs) throws JEVisException {
        try {
            _ds = ds;
            _name = rs.getString(COLUMN_NAME);
            _id = rs.getLong(COLUMN_ID);
            _link = rs.getLong(COLUMN_LINK);
//            _parentID = rs.getLong(COLUMN_PARENT);
            _class = rs.getString(COLUMN_CLASS);
            _classObj = new JEVisClassSQL(ds, _class);
//            _groupID = rs.getLong(COLUMN_GROUP);

            if (_class.equals("Link")) {//TODO: this is now done by the Repationship, remove
                _isLink = true;
                _linkedObject = ds.getObject(_link);
            }
        } catch (SQLException ex) {
            throw new JEVisException("Cannot parse Object", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }
    }

    public String getName() {
        return _name;
    }

    public void setName(String name) {
        _name = name;
        _hasChanged = true;
    }

    public Long getID() {
        return _id;
    }

    public JEVisClass getJEVisClass() throws JEVisException {
        if (isLink()) {
            return _linkedObject.getJEVisClass();
        }

        return _classObj;
    }

    @Override
    public List<JEVisObject> getParents() throws JEVisException {

        //TODO: implement the cach again
        //disabled the cach will be slower but faster
        _parentObjs = new LinkedList<JEVisObject>();

        if (_parentObjs == null) {
            for (JEVisRelationship rel : getRelationships(JEVisConstants.ObjectRelationship.PARENT)) {
                //find the relationshipts where we are the child
                if (rel.getStartObject().getID() == _id) {
                    if (RelationsManagment.canRead(_ds.getCurrentUser(), rel.getEndObject())) {
                        _childrenObj.add(rel.getEndObject());
                    }

                }
            }
        }

        return _parentObjs;
    }

    @Override
    public List<JEVisObject> getChildren() throws JEVisException {
        //TODO: implement the cach again
        //disabled the cach will be slower but faster
//        _childrenObj = new LinkedList<JEVisObject>();

        if (_childrenObj == null) {
            _childrenObj = new LinkedList<JEVisObject>();

            //TODO improve performance by fetching all object a an singel querry
            for (JEVisRelationship rel : getRelationships(JEVisConstants.ObjectRelationship.PARENT)) {
                //find the relationshipts where we are the parent
                if (rel.getEndObject().getID() == _id && rel.isType(JEVisConstants.ObjectRelationship.PARENT)) {
                    if (RelationsManagment.canRead(_ds.getCurrentUser(), rel.getStartObject())) {
                        _childrenObj.add(rel.getStartObject());
                    }

                }
            }

            Collections.sort(_childrenObj);
        }

        return _childrenObj;
    }

    @Override
    public List<JEVisObject> getChildren(JEVisClass jclass, boolean iherit) throws JEVisException {
        List<JEVisObject> chFromType = new ArrayList<JEVisObject>();

        List<JEVisClass> classes = new ArrayList<JEVisClass>();
        classes.add(jclass);

        if (iherit) {
            classes.addAll(jclass.getHeirs());
        }

        for (JEVisObject obj : getChildren()) {

            for (JEVisClass jc : classes) {
                if (obj.getJEVisClass().equals(jclass)) {
                    chFromType.add(obj);
                }

            }
        }

        Collections.sort(chFromType);

        return chFromType;
    }

    @Override
    public List<JEVisAttribute> getAttributes() throws JEVisException {
        if (isLink()) {
            return _linkedObject.getAttributes();
        }


        //Check if attributes are loaded
        //Workaround disable  this simple cach TODO:reimplement
        if (_attributes == null) {
            if (true) {
                AttributeTable adb = new AttributeTable(_ds);
                _attributes = adb.getAttributes(this);
            }


            //allow vaild types, add missing
            for (JEVisType type : getJEVisClass().getTypes()) {
                boolean isThere = false;
                for (JEVisAttribute att : _attributes) {
                    if (att.isType(type)) {
                        isThere = true;
                        break;
                    }
                }
                if (!isThere) {
                    //                System.out.println("add missing attribute: " + type.getName() + " in " + getID());
                    //TODO add commit to DB?
                    AttributeTable adb = new AttributeTable(_ds);
                    adb.insert(type, this);
                    _attributes.add(new JEVisAttributeSQL(_ds, this, type));//TODO unsave, better reload from DB?

                }

            }
            Collections.sort(_attributes);

        }


        return _attributes;
    }

    @Override
    public JEVisAttribute getAttribute(JEVisType type) throws JEVisException {
        if (isLink()) {
            return _linkedObject.getAttribute(type);
        }


        for (JEVisAttribute att : getAttributes()) {
            if (att.isType(type)) {
                return att;
            }
        }
//        JEVisAttribute emty = new JEVisAttributeSQL(_ds, null);

        return null; // or "Cannot exist" exception?

    }

    @Override
    public JEVisAttribute getAttribute(String type) throws JEVisException {
        JEVisType jtype = getJEVisClass().getType(type);

        return getAttribute(jtype); // or "Cannot exist" exception?

    }

    @Override
    public boolean delete() throws JEVisException {
        if (RelationsManagment.canDelete(_ds.getCurrentUser(), this)) {
            ObjectTable ot = new ObjectTable(_ds);

            for (JEVisObject parent : getParents()) {
                ((JEVisObjectSQL) parent).removeChild(this);
            }
            return true;

        } else {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }


    }

    protected boolean removeChild(JEVisObject object) {
        for (JEVisObject obj : _childrenObj) {
            if (obj.equals(object)) {
                _childrenObj.remove(obj);
                return true;
            }
        }
        return false;
    }

    public void moveTo(JEVisObject newParent) throws JEVisException {
        //TODO remimplement after Relationship changes
//
//        //check if the user has the right on both object
//        if (RelationsManagment.canDelete(_ds.getCurrentUser(), this)
//                || RelationsManagment.canCreate(_ds.getCurrentUser(), newParent)) {
//
//            //Check if new Parrent is a ok parent
//            if (getJEVisClass().isAllowedUnder(newParent.getJEVisClass())) {
//
//                JEVisObject oldParent = _parentObj;
//                //check if class is unique
//                if (getJEVisClass().isUnique()) {
//                    List<JEVisObject> brother = newParent.getChildren(this.getJEVisClass(), true);
//                    if (brother.size() > 0) {
//                        throw new JEVisException("Unique Class error", 23423954);
//                    }
//                } else {
//                    //do
//                    setParent(newParent);
//                    commit();
//                    //TODo make a better/saver implementation
//                    if (getParent() instanceof JEVisObjectSQL) {
//                        JEVisObjectSQL tmp = (JEVisObjectSQL) newParent;
//                        tmp._childrenObj.remove(this);
//                    }
//
//
//                    if (newParent instanceof JEVisObjectSQL) {
//                        JEVisObjectSQL tmp = (JEVisObjectSQL) newParent;
//                        tmp._childrenObj.add(this);
//                    }
//
//                }
//            } else {
//                throw new JEVisException("JEVisClass is now alloowed under this parent", 23423955);
//            }
//        } else {
//            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
//        }
    }

    @Override
    public JEVisObject buildObject(String name, JEVisClass jclass) throws JEVisException {
        if (RelationsManagment.canCreate(_ds.getCurrentUser(), this)) {

            if (jclass.isAllowedUnder(getJEVisClass())) {

                //User is spezial case and can only be once in the system with the same name
                if (jclass.getName().equals(JEVisConstants.Class.USER)) {
                    if (!_ds.getObjectTable().isUserUnique(name)) {
                        throw new JEVisException("Can not create User with this name. The User has to be unique on the System", 85392);
                    }
                }

                //check if class is unique               
                if (jclass.isUnique()) {

                    List<JEVisObject> brother = getChildren(this.getJEVisClass(), true);
                    if (brother.size() > 0) {
                        throw new JEVisException(jclass.getName() + " has to be unique under parent", 23423954);
                    } else {
                        return insert(name, jclass);
                    }
                } else {
                    return insert(name, jclass);
                }
            } else {
                throw new JEVisException(jclass.getName() + " is not allowed under " + getJEVisClass().getName(), 23423955);
            }
        } else {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }
    }

    private JEVisObject insert(String name, JEVisClass jclass) throws JEVisException {
        JEVisObject newObj = _ds.getObjectTable().insertObject(name, jclass, this, _groupID);
        getChildren().add(newObj);

        List<JEVisClassRelationship> crels = getJEVisClass().getRelationships(
                JEVisConstants.ClassRelationship.NESTEDT,
                JEVisConstants.Direction.BACKWARD);

        for (JEVisClassRelationship crel : crels) {//TODo: test
            newObj.buildObject(jclass.getName(), crel.getOtherClass((JEVisClass) this.getJEVisClass()));
        }


        return newObj;
    }

    @Override
    public JEVisDataSource getDataSource() throws JEVisException {
        return _ds;
    }

    @Override
    public boolean equals(Object o) {
        try {
            if (o instanceof JEVisObject) {
                JEVisObject obj = (JEVisObject) o;
                if (obj.getID().equals(_id)) {
                    return true;
                }
            }
        } catch (Exception ex) {
            System.out.println("error, cannot compare objects");
            return false;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 67 * hash + (int) (this._id ^ (this._id >>> 32));
        return hash;
    }

    @Override
    public String toString() {
        if (isLink()) {
            try {
                return "JEVisObjectSQL{" + "name=" + _name + ", id=" + _id + ", link=" + _link + ", parentID=" + _parentID
                        + ", class=" + _linkedObject.getJEVisClass().getName()
                        + ", groupID=" + _groupID + '}';

            } catch (Exception ex) {
                ex.printStackTrace();
            }

        }

        return "JEVisObjectSQL{" + "name=" + _name + ", id=" + _id + ", link=" + _link + ", class=" + _class + '}';
    }

    @Override
    public boolean isLink() {
//        List<JEVisRelationship> rels = RelationsManagment.getRelationByType(this, JEVisConstants.RELATIONSHIP_TYPE_LINK);
//        for (JEVisRelationship r : rels) {
//            if (r.getStartObject().getID() == _id) {
//                return true;
//            }
//        }
        return false;
    }

    @Override
    public JEVisObject getLinkedObject() throws JEVisException {
        return _linkedObject;
    }

    @Override
    public void commit() throws JEVisException {
        if (_hasChanged) {
            //User is spezial case and can only be once in the system with the same name
            if (getJEVisClass().getName().equals(JEVisConstants.Class.USER)) {
                if (!_ds.getObjectTable().isUserUnique(_name)) {
                    throw new JEVisException("Can not create User with this name. The User has to be unique on the System", 85392);
                }
            }

            _ds.getObjectTable().updateObject(this);
            _hasChanged = false;
        }
    }

    @Override
    public void rollBack() {
        try {
            JEVisObject obj = _ds.getObjectTable().getObject(_id, false);
            _name = obj.getName();
            //TODO rest
        } catch (JEVisException ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public boolean hasChanged() {
        return _hasChanged;
    }

    @Override
    public JEVisRelationship buildRelationship(JEVisObject obj, int type, int direction) throws JEVisException {
        //TODO: add the checking if the Relationship is OK mybe at the JEViCalss level?

        if (!RelationsManagment.canCreate(_ds.getCurrentUser(), this)
                || !RelationsManagment.canCreate(_ds.getCurrentUser(), obj)) {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }

        JEVisRelationship newRel = null;
        if (direction == JEVisConstants.Direction.FORWARD) {
            newRel = _ds.getRelationshipTable().insert(_id, obj.getID(), type);
        } else {
            newRel = _ds.getRelationshipTable().insert(obj.getID(), _id, type);
        }

        _relationships.add(newRel);
        return newRel;
    }

    @Override
    public void deleteRelationship(JEVisRelationship rel) throws JEVisException {
        if (!RelationsManagment.canWrite(_ds.getCurrentUser(), this)
                || !RelationsManagment.canWrite(_ds.getCurrentUser(), rel.getOtherObject(this))) {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }

        if (rel.getStartObject().getID() == _id || rel.getEndObject().getID() == _id) {
            if (!_ds.getRelationshipTable().delete(rel)) {
                throw new JEVisException("Could not delete Relationship", 342385);
            } else {
                _relationships.remove(rel);
            }
        } else {
            throw new JEVisException("This relationship does not belong to this object", 689355);
        }
    }

    @Override
    public List<JEVisRelationship> getRelationships() throws JEVisException {
        //ToDo: care for userrights
        return _relationships;
    }

    @Override
    public List<JEVisRelationship> getRelationships(int type) throws JEVisException {
        List<JEVisRelationship> tmp = new LinkedList<JEVisRelationship>();
        for (JEVisRelationship rel : _relationships) {
            if (rel.isType(type)) {
                tmp.add(rel);
            }
        }
        return tmp;
    }

    @Override
    public List<JEVisRelationship> getRelationships(int type, int direction) throws JEVisException {
        List<JEVisRelationship> tmp = new LinkedList<JEVisRelationship>();
        for (JEVisRelationship rel : _relationships) {
            if (rel.isType(type)) {
                if (rel.getStartObject().getID() == _id && direction == JEVisConstants.Direction.FORWARD) {
                    tmp.add(rel);
                } else if (rel.getEndObject().getID() == _id && direction == JEVisConstants.Direction.BACKWARD) {
                    tmp.add(rel);
                }
            }
        }
        return tmp;
    }

    protected void addRelationship(JEVisRelationship rel) throws JEVisException {
        logger.debug("Add relationship to object: {}", rel);
        //TODO: mabe check if this relation is ok, but this is protected so no prio
        _relationships.add(rel);
    }

    @Override
    public int compareTo(JEVisObject o) {
        return getName().compareTo(o.getName());
    }
}
