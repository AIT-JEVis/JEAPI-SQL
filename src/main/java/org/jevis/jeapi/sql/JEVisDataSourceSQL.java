/**
 * Copyright (C) 2013 - 2014 Envidatec GmbH <info@envidatec.com>
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisConstants;
import org.jevis.jeapi.JEVisDataSource;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisExceptionCodes;
import org.jevis.jeapi.JEVisObject;
import org.jevis.jeapi.JEVisRelationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The JEVisDataSource implements the JEVisDataSource to work with an MySQL
 * database.
 *
 *
 * @author Florian Simon<florian.simon@envidatec.com>
 */
public class JEVisDataSourceSQL implements JEVisDataSource {

    private Connection _connect = null;
    private String _dbHost = "";
    private String _dbPort = "";
    private String _dbUser = "";
    private String _dbPW = "";
    private String _dbSchema = "";
    private String _jevisUsername;
    private String _jevisUserPW;
    private JEVisObject _user;
    private Logger logger = LoggerFactory.getLogger(JEVisDataSourceSQL.class);
    private ObjectTable _ot;
    private ClassTable _ct;
    private AttributeTable _at;
    private RelationshipTable _rt;
    private ClassRelationTable _crt;
    private int qCount = 0;//for benchmarking

    public JEVisDataSourceSQL(String db, String port, String schema, String user, String pw, String jevisUser, String jevisPW) throws JEVisException {
        _dbHost = db;
        _dbPort = port;
        _dbUser = user;
        _dbPW = pw;
        _dbSchema = schema;
        _jevisUsername = jevisUser;
        _jevisUserPW = jevisPW;
//        getConnection();
    }

    @Override
    public boolean connect(String username, String password) throws JEVisException {
        _jevisUsername = username;
        _jevisUserPW = password;
        if (connectToDB()) {
            System.out.println("DB connection is OK login user");
            loginUser();//throw exeption is something is wrong 
            return true;
        } else {
            throw new JEVisException("Error DataSource is not connected ", 2134);
        }
    }

    protected Connection getConnection() throws JEVisException {
        return _connect;
    }

    @Override
    public JEVisObject getCurrentUser() throws JEVisException {
        return _user;
    }

    public boolean connectToDB() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
//
//            _connect = DriverManager
//                    .getConnection("jdbc:mysql://" + _dbHost +":"+_dbPort+ "/" + _dbSchema + "?"
//                    + "user=" + _dbUser + "&password=" + _dbPW + "&serverTimezone=UTC" + "&useGmtMillisForDatetimes=true"
//                    + "&useJDBCCompliantTimezoneShift=true" + "&useLegacyDate‌timeCode=false");

            String conSring = "jdbc:mysql://" + _dbHost + ":" + _dbPort + "/" + _dbSchema + "?"
                    + "user=" + _dbUser + "&password=" + _dbPW;
            logger.info("Using Connection string: {}", conSring);

            _connect = DriverManager.getConnection(conSring);
            if (_connect.isValid(2000)) {
                return true;
            } else {
                return false;
            }



        } catch (Exception ex) {
            logger.error("Error while connecting to DB: {}", ex.getMessage());
            return false;
        }
    }

    public boolean closeConnection() {
        if (_connect != null) {
            try {
                _connect.close();
                return true;
            } catch (SQLException ex) {
                logger.warn("Error while closing DB connection: {}", ex);
                return false;
            }
        }
        return true;
    }

    private void loginUser() throws JEVisException {
        logger.info("Try to login user: {}", _jevisUsername);

        try {
            _user = getObjectTable().loginUser(_jevisUsername, _jevisUserPW);
            if (_user != null) {
                logger.info("Login OK for {}", _jevisUsername);

            } else {
                throw new JEVisException("User does not exist or password was wrong", JEVisExceptionCodes.UNAUTHORIZED);
            }

        } catch (Exception ex) {
            throw new JEVisException(ex.getMessage(), JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }
    }

    @Override
    public JEVisClass getJEVisClass(String name) throws JEVisException {
        JEVisClass jClass = getClassTable().getObjectClass(name, true);
        return jClass;
    }

    @Override
    public List<JEVisClass> getJEVisClasses() throws JEVisException {
        try {
            List<JEVisClass> jClass = getClassTable().getAllObjectClasses();
            Collections.sort(jClass);
            return jClass;
        } catch (SQLException ex) {
            throw new JEVisException("error while select object class from JEVis MySQL Datsource  ", JEVisExceptionCodes.DATASOURCE_FAILD, ex);
        }
    }

    @Override
    public JEVisClass buildClass(String name) throws JEVisException {
        try {
            if (RelationsManagment.isSysAdmin(_user)) {
                if (getClassTable().insert(name)) {
                    return getJEVisClass(name);
                } else {
                    throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
                }
            } else {
                throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
            }

        } catch (Exception ex) {
            throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED, ex);
        }
    }

    @Override
    public List<JEVisObject> getRootObjects() throws JEVisException {
        List<JEVisObject> roots = new LinkedList<JEVisObject>();
        List<JEVisRelationship> member = RelationsManagment.getRelationByType(getCurrentUser(), JEVisConstants.ObjectRelationship.MEMBER_READ);
        for (JEVisRelationship rel : member) {
            JEVisObject group = rel.getOtherObject(getCurrentUser());
            List<JEVisRelationship> root = RelationsManagment.getRelationByType(group, JEVisConstants.ObjectRelationship.ROOT);
            for (JEVisRelationship rel2 : root) {
                JEVisObject obj = rel2.getOtherObject(group);
                roots.add(obj);
            }
        }

        for (JEVisRelationship rel : RelationsManagment.getRelationByType(_user, JEVisConstants.ObjectRelationship.ROOT)) {
            roots.add(rel.getOtherObject(getCurrentUser()));
        }
        Collections.sort(roots);

        return roots;
    }

    @Override
    public JEVisObject getObject(Long id) throws JEVisException {
        try {
            JEVisObject obj = null;

            obj = getObjectTable().getObject(id, true);

            if (RelationsManagment.canRead(_user, obj)) {
                return obj;
            } else {
                throw new JEVisException("Access not allowed to the object " + id + " from user " + getCurrentUser().getName(), JEVisExceptionCodes.UNAUTHORIZED);
            }


        } catch (Exception ex) {
            ex.printStackTrace();
            throw new JEVisException("error while select objetc from JEVis MySQL Datsource  ", JEVisExceptionCodes.DATASOURCE_FAILD, ex);
        }
    }

    public void deleteObject(long id) throws JEVisException {
        try {
            JEVisObject obj = getObject(id);
            if (RelationsManagment.canDelete(_user, obj)) {
                getObjectTable().deleteObject(obj);

            } else {
                throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
            }

        } catch (Exception ex) {
            throw new JEVisException("error while insert object into JEVis MySQL Datsource  ", JEVisExceptionCodes.DATASOURCE_FAILD, ex);
        }
    }

    @Override
    public JEVisObject buildLink(String name, JEVisObject parent, JEVisObject linkedObject) throws JEVisException {
        try {
            if (RelationsManagment.canCreate(_user, parent)) {
                return getObjectTable().insertLink(name, parent, linkedObject);
            } else {
                throw new JEVisException("Unsifficent rights", JEVisExceptionCodes.UNAUTHORIZED);
            }

        } catch (Exception ex) {
            throw new JEVisException("error while insert object into JEVis MySQL Datsource  ", JEVisExceptionCodes.DATASOURCE_FAILD, ex);
        }

    }

    @Override
    public List<JEVisObject> getObjects(JEVisClass jevisClass, boolean inherits) throws JEVisException {
//        System.out.println("getObject by class: " + jevisClass.getName() + " heirs: " + inherits);
        List<JEVisClass> classes = new ArrayList<JEVisClass>();
        classes.add(jevisClass);
        if (inherits) {
            classes.addAll(jevisClass.getHeirs());
        }

        List<JEVisObject> objs = getObjectTable().getObjects(classes);
        Collections.sort(objs);

        return objs;
    }

    protected ObjectTable getObjectTable() throws JEVisException {
        if (_ot == null) {
            _ot = new ObjectTable(this);
        }
        return _ot;
    }

    protected ClassTable getClassTable() throws JEVisException {
        if (_ct == null) {
            _ct = new ClassTable(this);
        }
        return _ct;
    }

    protected RelationshipTable getRelationshipTable() throws JEVisException {
        if (_rt == null) {
            _rt = new RelationshipTable(this);
        }
        return _rt;
    }

    @Override
    public List<JEVisRelationship> getReplationships(int type) throws JEVisException {
        //TODO implement the userrights
        return getRelationshipTable().select(type);
    }

    protected AttributeTable getAttributeTable() throws JEVisException {
        if (_at == null) {
            _at = new AttributeTable(this);
        }
        return _at;
    }

    protected ClassRelationTable getClassRelationshipTable() throws JEVisException {
        if (_crt == null) {
            _crt = new ClassRelationTable(this);
        }
        return _crt;
    }

    protected void addQuery() {
        qCount++;
    }

    public int getCount() {
        int tmp = qCount;
        qCount = 0;
        return tmp;
    }
}
