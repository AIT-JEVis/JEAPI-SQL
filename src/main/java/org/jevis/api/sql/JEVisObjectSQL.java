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
package org.jevis.api.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import org.jevis.api.JEVisAttribute;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisClassRelationship;
import org.jevis.api.JEVisConstants;
import org.jevis.api.JEVisDataSource;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisExceptionCodes;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisRelationship;
import org.jevis.api.JEVisType;

import static org.jevis.api.sql.ObjectTable.COLUMN_ID;
import static org.jevis.api.sql.ObjectTable.COLUMN_NAME;
import static org.jevis.api.sql.ObjectTable.COLUMN_CLASS;
import org.jevis.commons.CommonClasses;

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
    private long _parentID;
    private List<JEVisObject> _parentObjs;
    private String _class;
    private long _groupID;
    private JEVisDataSourceSQL _ds;
    private List<JEVisObject> _childrenObj;
    private JEVisClass _classObj;
    private List<JEVisAttribute> _attributes;
    private List<JEVisRelationship> _relationships = new LinkedList<JEVisRelationship>();
    private JEVisObject _linkedObject = null;
    private Logger logger = LoggerFactory.getLogger(JEVisDataSourceSQL.class);

    public JEVisObjectSQL(JEVisDataSourceSQL ds, ResultSet rs) throws JEVisException {
        try {
            _ds = ds;
            _name = rs.getString(COLUMN_NAME);
            _id = rs.getLong(COLUMN_ID);
//            _link = rs.getLong(COLUMN_LINK);
//            _parentID = rs.getLong(COLUMN_PARENT);
            _class = rs.getString(COLUMN_CLASS);
            _classObj = new JEVisClassSQL(ds, _class);
//            _groupID = rs.getLong(COLUMN_GROUP);

//            if (_class.equals("Link")) {//TODO: this is now done by the Repationship, remove
//                _isLink = true;
////                _linkedObject = ds.getObject(_link);
//            }
        } catch (SQLException ex) {
            throw new JEVisException("Cannot parse Object", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public void setName(String name) {
        _name = name;
        _hasChanged = true;
    }

    @Override
    public Long getID() {
        return _id;
    }

    @Override
    public JEVisClass getJEVisClass() throws JEVisException {
//        if (isLink()) {
//            return _linkedObject.getJEVisClass();
//        }

        return _classObj;
    }

    @Override
    public List<JEVisObject> getParents() throws JEVisException {

        //TODO: implement the cach again
        //disabled the cach will be slower but faster
//        if (_parentObjs == null) {
        _parentObjs = new LinkedList<JEVisObject>();
        for (JEVisRelationship rel : getRelationships(JEVisConstants.ObjectRelationship.PARENT, JEVisConstants.Direction.BACKWARD)) {
            //find the relationshipts where we are the child
            if (rel.getStartObject().getID() == _id) {
                if (RelationsManagment.canRead(_ds.getCurrentUser(), rel.getEndObject())) {
                    _parentObjs.add(rel.getEndObject());
                }
            }
        }
//        }

        return _parentObjs;
    }

    @Override
    public List<JEVisObject> getChildren() throws JEVisException {

//        printRelationships();//debug
        if (_childrenObj == null) {
            _childrenObj = new LinkedList<JEVisObject>();

            for (JEVisRelationship rel : getRelationships(JEVisConstants.ObjectRelationship.PARENT, JEVisConstants.Direction.BACKWARD)) {
                if (RelationsManagment.canRead(_ds.getCurrentUser(), rel.getStartObject())) {

                    _childrenObj.add(rel.getStartObject());
                } else {
                    System.out.println("Has NO access to: " + rel.getStartObject());
                }

            }

            Collections.sort(_childrenObj);
        }

        return _childrenObj;
    }

    //Debug helper
    private void printRelationships() {
        try {
            String typeName = "";
            for (JEVisRelationship rel : getRelationships()) {
                switch (rel.getType()) {
                    case 1:
                        typeName = "Parent";
                        break;
                    case 2:
                        typeName = "Link";
                        break;
                    case 3:
                        typeName = "Root";
                        break;
                    case 4:
                        typeName = "Source";
                        break;
                    case 5:
                        typeName = "Service";
                        break;
                    case 6:
                        typeName = "Input";
                        break;
                    case 7:
                        typeName = "Data";
                        break;
                    case 100:
                        typeName = "Owner";
                        break;
                    case 101:
                        typeName = "Member - Read";
                        break;
                    case 102:
                        typeName = "Member - Write";
                        break;
                    case 103:
                        typeName = "Member - Exe";
                        break;
                    case 104:
                        typeName = "Member - Create";
                        break;
                    case 105:
                        typeName = "Member - Delete";
                        break;
                }
                String msg = String.format("+rel: from [%s]%s to [%s]%s type: %S", rel.getStartObject().getID(), rel.getStartObject().getName(), rel.getEndObject().getID(), rel.getEndObject().getName(), typeName);
                System.out.println(msg);
            }
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(JEVisObjectSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
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
//        if (isLink()) {
//            System.out.println("getLink.getAttributes(): " + this.getID());
//            return _linkedObject.getAttributes();
//        }

        //Check if attributes are loaded
        //Workaround disable  this simple cach TODO:reimplement
        if (_attributes == null) {
            _attributes = new LinkedList<JEVisAttribute>();
            AttributeTable adb = _ds.getAttributeTable();
            //allow onl y vaild types, add missing
            for (JEVisType type : getJEVisClass().getTypes()) {
                boolean isThere = false;
                for (JEVisAttribute att : adb.getAttributes(this)) {
                    if (att.isType(type)) {
                        isThere = true;
                        _attributes.add(att);
                        break;
                    }
                }
                if (!isThere) {
                    //TODO add commit to DB?
                    adb.insert(type, this);
                    _attributes.add(new JEVisAttributeSQL(_ds, this, type));//TODO unsave, better reload from DB?

                }
            }
            Collections.sort(_attributes);
        }

//        //Check if attributes are loaded
//        //Workaround disable  this simple cach TODO:reimplement
//        if (_attributes == null) {
//            if (true) {
//                AttributeTable adb = new AttributeTable(_ds);
//                _attributes = adb.getAttributes(this);
//            }
//
//            //allow onl y vaild types, add missing
//            for (JEVisType type : getJEVisClass().getTypes()) {
//                boolean isThere = false;
//                for (JEVisAttribute att : _attributes) {
//                    if (att.isType(type)) {
//                        isThere = true;
//                        break;
//                    }
//                }
//                if (!isThere) {
//                    //TODO add commit to DB?
//                    AttributeTable adb = new AttributeTable(_ds);
//                    adb.insert(type, this);
//                    _attributes.add(new JEVisAttributeSQL(_ds, this, type));//TODO unsave, better reload from DB?
//
//                }
//            }
//            Collections.sort(_attributes);
//        }
        return _attributes;
    }

    @Override
    public JEVisAttribute getAttribute(JEVisType type) throws JEVisException {
//        if (isLink()) {
//            return _linkedObject.getAttribute(type);
//        }

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

            if (ot.deleteObject(this)) {
                for (JEVisObject parent : getParents()) {
                    ((JEVisObjectSQL) parent).removeChild(this);
                }
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
        System.out.println("buildObject: " + name + " | " + jclass);

        //TODO: is this nessasary now?
        if (getJEVisClass().equals(CommonClasses.LINK.NAME)) {
            throw new JEVisException("Can not create an object under an Link", 85393);
        }

        System.out.println("Can create: " + RelationsManagment.canCreate(_ds.getCurrentUser(), this));
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
                JEVisConstants.ClassRelationship.NESTED,
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
//        if (isLink()) {
//            try {
//                return "JEVisObjectSQL{" + "name=" + _name + ", id=" + _id + ", link=" + isLink() + ", parentID=" + _parentID
//                        + ", class=" + _linkedObject.getJEVisClass().getName()
//                        + ", groupID=" + _groupID + '}';
//
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//
//        }

        return "JEVisObjectSQL{" + "name=" + _name + ", id=" + _id + ", link=" + "?" + ", class=" + _class + '}';
    }

//    @Override
//    public boolean isLink() {
//        try {
//            for (JEVisRelationship rel : _relationships) {//(JEVisConstants.ObjectRelationship.LINK, JEVisConstants.Direction.FORWARD)) {
//                if (rel.getType() == JEVisConstants.ObjectRelationship.LINK && rel.getStartObject().equals(this)) {
//                    System.out.println("is link: " + rel);
//                    return true;
//                }
//
//            }
//        } catch (JEVisException ex) {
//            java.util.logging.Logger.getLogger(JEVisObjectSQL.class.getName()).log(Level.SEVERE, null, ex);
//        }
//        return false;
//    }
    @Override
    public JEVisObject getLinkedObject() throws JEVisException {
        try {
            for (JEVisRelationship rel : getRelationships(JEVisConstants.ObjectRelationship.LINK, JEVisConstants.Direction.FORWARD)) {
//                System.out.println("return link object: " + rel.getEndObject());
                return rel.getEndObject();
            }
        } catch (JEVisException ex) {
            java.util.logging.Logger.getLogger(JEVisObjectSQL.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

//    @Override
//    public JEVisObject buildLink(String name, JEVisObject parent) throws JEVisException {
//        JEVisObject newObjLink = parent.buildObject(name, _ds.getJEVisClass("Link"));
//
//        if (newObjLink != null) {
//            buildRelationship(parent, JEVisConstants.ObjectRelationship.LINK, JEVisConstants.Direction.FORWARD);
//        }
//
//        return null;
//    }
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

        //TODO: Simple cache cleaning, redo
        if (newRel.getType() == JEVisConstants.ObjectRelationship.PARENT) {
            _childrenObj = null;
        }

        return newRel;
    }

    @Override
    public void deleteRelationship(JEVisRelationship rel) throws JEVisException {
        System.out.println("\n\ndeleteRelationship: " + rel);
        System.out.println("This: " + this);
        System.out.println("start: " + rel.getStartObject() + "\nend: " + rel.getEndObject());
        System.out.println("need write to: " + rel.getOtherObject(this));

        //we only need the permisson for this object        
        if (!RelationsManagment.canWrite(_ds.getCurrentUser(), this)) {
//                || !RelationsManagment.canWrite(_ds.getCurrentUser(), rel.getOtherObject(this))) {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
        }

        //TODO i think we habe to make an additonal security check for userright relationships
        if (rel.getType() == JEVisConstants.ObjectRelationship.OWNER
                || rel.getType() == JEVisConstants.ObjectRelationship.MEMBER_CREATE
                || rel.getType() == JEVisConstants.ObjectRelationship.MEMBER_DELETE
                || rel.getType() == JEVisConstants.ObjectRelationship.MEMBER_EXECUTE
                || rel.getType() == JEVisConstants.ObjectRelationship.MEMBER_READ
                || rel.getType() == JEVisConstants.ObjectRelationship.MEMBER_WRITE) {
            //TODO hndel this rights
            System.out.println("WARNING user trys to delete userright but checking is not enabled: " + rel);
            //throw JEVisException
        }

        System.out.println("can delete relationship: " + rel);
        System.out.println("this object: " + _id);

        boolean isOK = false;
        for (JEVisRelationship orgRel : _relationships) {
            if (orgRel.equals(rel)) {
                isOK = true;
                break;
            }
        }
        if (isOK) {
            if (!_ds.getRelationshipTable().delete(rel)) {
                throw new JEVisException("Could not delete Relationship", 342385);
            } else {
                System.out.println("delete done");
                _relationships.remove(rel);
////
////                if (rel.isType(JEVisConstants.ObjectRelationship.PARENT)) {
////                    dsf
////                }
            }
        } else {
            throw new JEVisException("This relationship does not belong to this object", 689355);
        }

//        if (rel.getStartObject().getID() == _id || rel.getEndObject().getID() == _id) {
//            System.out.println("is part of this object");
//            if (!_ds.getRelationshipTable().delete(rel)) {
//                throw new JEVisException("Could not delete Relationship", 342385);
//            } else {
//                _relationships.remove(rel);
//            }
//        } else {
//            throw new JEVisException("This relationship does not belong to this object", 689355);
//        }
    }

    @Override
    public List<JEVisRelationship> getRelationships() throws JEVisException {
        //ToDo: care for userrights
        return _relationships;
    }

    @Override
    public boolean isAllowedUnder(JEVisObject otherObject) throws JEVisException {
//        if (isLink() && otherObject.getJEVisClass().equals(_ds.getJEVisClass(CommonClasses.LINK.NAME))) {
//            return true;
//        } else if (isLink() && otherObject.isLink()) {
//
//        }

        for (JEVisClassRelationship rel : getJEVisClass().getRelationships(JEVisConstants.ClassRelationship.OK_PARENT, JEVisConstants.Direction.FORWARD)) {
            System.out.println("isAllowedUnder: " + rel);
            System.out.println("Rel End: " + rel.getEnd());
            if (rel.getEnd().equals(otherObject.getJEVisClass())) {
                System.out.println("is Allowed");
                return true;

            }
        }
        return false;
    }

    @Override
    public List<JEVisRelationship> getRelationships(int type) throws JEVisException {
        System.out.println("getRelationships(int)");
        List<JEVisRelationship> tmp = new LinkedList<JEVisRelationship>();
        for (JEVisRelationship rel : getRelationships()) {
            if (rel.isType(type)) {
                tmp.add(rel);
            }
        }
        return tmp;
    }

    @Override
    public List<JEVisRelationship> getRelationships(int type, int direction) throws JEVisException {
        List<JEVisRelationship> tmp = new LinkedList<JEVisRelationship>();
        for (JEVisRelationship rel : getRelationships()) {
            try {
//                System.out.println("#: " + rel);
//                System.out.println("type: " + rel.isType(type));
//                System.out.println("startObj: " + rel.getStartObject());
//                System.out.println("endObject: " + rel.getEndObject());

                if (rel.isType(type)) {
                    if (rel.getStartObject().getID() == _id && direction == JEVisConstants.Direction.FORWARD) {
                        tmp.add(rel);
                    } else if (rel.getEndObject().getID() == _id && direction == JEVisConstants.Direction.BACKWARD) {
                        tmp.add(rel);
                    }
                }
            } catch (Exception ex) {
                //TODO: handel this excetption, most likly there is an error in the api
                System.out.println("getRelationships(int,int).error: " + ex);
            }
        }
        return tmp;
    }

    protected void addRelationship(JEVisRelationship rel) throws JEVisException {
        logger.debug("Add relationship to object: {}", rel);
        //TODO: mabe check if this relation is ok, but this is protected so no prio
        getRelationships().add(rel);
    }

    @Override
    public int compareTo(JEVisObject o) {
        return getName().compareTo(o.getName());
    }

    @Override
    public List<JEVisClass> getAllowedChildrenClasses() throws JEVisException {
        List<JEVisClassRelationship> rels = getJEVisClass().getRelationships(
                JEVisConstants.ClassRelationship.OK_PARENT,
                JEVisConstants.Direction.BACKWARD);
        List<JEVisClass> okClasses = new LinkedList<JEVisClass>();
        for (JEVisClassRelationship rel : rels) {
            okClasses.add(rel.getOtherClass(getJEVisClass()));
        }

        return okClasses;
    }
}
