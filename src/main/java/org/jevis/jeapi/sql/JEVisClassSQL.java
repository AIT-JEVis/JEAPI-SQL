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
import java.util.List;
import java.util.Locale;
import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisDataSource;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisExceptionCodes;
import org.jevis.jeapi.JEVisType;

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
    private List<Relation> _relations;
    private JEVisClass _inheritance;
    private List<JEVisClass> _okParents;
    private List<JEVisClass> _oldOkParent;
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

            _inheritance = new JEVisClassSQL(ds, rs.getString(ClassTable.COLUMN_INHERIT));
            _unique = rs.getBoolean(ClassTable.COLUMN_UNIQUE);
//            System.out.println(_name+ " isUnique "+_unique);


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

    public String getName() {
        return _name;
    }

    public void setName(String name) {
//        _oldName= _name;
        _name = name;
        _hasChanged = true;
    }

    public ImageIcon getIcon() {
        return _icon;
    }

    public void setIcon(ImageIcon icon) {
        _icon = icon;
        _hasChanged = true;
    }

    public String getDescription() {
        return _discription;
    }

    public void setDescription(String discription) {
        _discription = discription;
        _hasChanged = true;
    }

    private List<Relation> getRelations() throws JEVisException {
        if (_relations == null) {
            try {
                ClassRelationTable crt = new ClassRelationTable(_ds);
                _relations = crt.get(this);
            } catch (Exception ex) {
                throw new JEVisException("Eroor while loading class relations", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
            }
        }
        return _relations;

    }

    public JEVisClass getInheritance() {
        return _inheritance;
    }

    public void setInheritance(JEVisClass jevisClass) {
        //TODO Check is it ok to inherit? 
        //TODO Check endles loop
        try {
            _hasChanged = true;
            _inheritance = jevisClass;

        } catch (Exception ex) {
            System.out.println("Error while Adding an Inherited JEVisClass: " + ex);
        }
    }

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

    public boolean isAllowedUnder(JEVisClass jevisClass) {
        for (JEVisClass pClass : getValidParents()) {
            if (pClass.equals(jevisClass)) {
                return true;
            }
        }



        return false;
    }

    public List<JEVisType> getTypes() {
        if (_types == null) {
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

    public JEVisType getType(String typename) {
        for (JEVisType ty : getTypes()) {
            if (ty.getName().equals(typename)) {
                return ty;
            }
        }
        return null;
    }

    /**
     *
     * @return
     */
    public List<JEVisClass> getValidParents() {
        if (_okParents == null) {
            try {
                _okParents = new ArrayList<JEVisClass>();
                List<Relation> relations = getRelations();

                for (Relation rel : relations) {
                    if (rel.isValidParent()) {
                        _okParents.add(new JEVisClassSQL(_ds, rel.getValidParent()));
                    }
                }

                if (getInheritance() != null) {
                    if (!getInheritance().equals(this)) {
                        _okParents.addAll(getInheritance().getValidParents());
                    }
                }


            } catch (Exception ex) {
                System.out.println("Error while loading Inherited Classes: " + ex);
                ex.printStackTrace();
            }
        }
        if (_okParents == null) {
            System.out.println("return emty getValidParents List");
            _okParents = new ArrayList<JEVisClass>();
        }

        return _okParents;
    }

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

        //check if the Vaild Parenship has changed
        if (_oldOkParent != null) {
            System.out.println("Valid parents have changed");

            //find delete Classes
            for (JEVisClass jc : _oldOkParent) {
                boolean exist = false;
                for (JEVisClass jsNeu : _okParents) {
                    if (jsNeu.equals(jc)) {
                        exist = true;
                    }
                }

                if (!exist) {
                    //delete
                    //ToDo delete
                }
            }

            //find new classes
            for (JEVisClass jsNeu : _okParents) {
                boolean exist = false;
                System.out.print("is '" + jsNeu.getName() + "' new?\n  ");
                for (JEVisClass jc : _oldOkParent) {
                    System.out.print(" .. " + jc.getName());
                    if (jsNeu.equals(jc)) {
                        exist = true;
                        System.out.print(" .. no");
                        break;
                    }
                }

                if (!exist) {
                    //add
                    System.out.println(" .. yes");
                    System.out.println("add new Parents");
                    ClassRelationTable crt = new ClassRelationTable(_ds);
                    crt.putVaildParent(this, jsNeu);
                }
                System.out.println("");
            }

            //ToDo: fix the cached heirs ther ok parents also changed


        }

        _oldOkParent = null;
        _hasChanged = false;
    }

    public boolean hasChanged() {
        return _hasChanged;
    }

    public void rollBack() {
        try {
            JEVisClass original = _ds.getJEVisClass(_name);
            _name = original.getName();
            _discription = original.getDescription();

            _icon = original.getIcon();
            _inheritance = original.getInheritance();

            _hasChanged = false;
            _oldOkParent = null;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void removeValidParent(JEVisClass jevisClass) {
        _hasChanged = true;
        getValidParents();
        if (_oldOkParent == null) {
            _oldOkParent = new ArrayList<JEVisClass>(_okParents);
        }
        _okParents.remove(jevisClass);
    }

    /**
     * TODO: check if parent is vails/exists
     *
     * @param jevisClass
     */
    public void addValidParent(JEVisClass jevisClass) {
        _hasChanged = true;
        getValidParents();
//        System.out.println("VaildParentCount: "+_okParents.size());
        if (_oldOkParent == null) {
//            System.out.println("_oldOkParent is null");
            _oldOkParent = new ArrayList<JEVisClass>(_okParents);
        }
        _okParents.add(jevisClass);
//        System.out.println("_oldOkParent.size: "+_oldOkParent.size());

    }

    public boolean isUnique() {
        return _unique;
    }

    public void setUnique(boolean unique) {
        _hasChanged = true;
        _unique = unique;
    }

    @Override
    public String toString() {
        String inherit = "null";
        String discription = "null";
        if (_inheritance != null) {
            inherit = _inheritance.getName();
        }
        if (_discription != null) {
            discription = _discription;
        }
        return "JEVisClassSQL{" + " name=" + _name + ",discription=" + discription + ", inheritance=" + inherit + ", unique=" + _unique + '}';
    }

    public List<JEVisClass> getHeirs() throws JEVisException {
        ClassTable ct = new ClassTable(_ds);
        return ct.getAllHeirs(this);
    }
}
