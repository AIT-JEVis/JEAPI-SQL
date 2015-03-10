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
package org.jevis.api.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import org.jevis.api.JEVisClass;
import org.jevis.api.JEVisConstants;
import org.jevis.api.JEVisDataSource;
import org.jevis.api.JEVisException;
import org.jevis.api.JEVisExceptionCodes;
import org.jevis.api.JEVisInfo;
import org.jevis.api.JEVisObject;
import org.jevis.api.JEVisRelationship;
import org.jevis.api.JEVisUnit;
import org.jevis.commons.unit.JEVisUnitImp;
import org.jevis.commons.utils.Benchmark;
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
    private boolean ssl = false;

    private List<JEVisObject> _objectChache = new ArrayList<JEVisObject>();

    final private JEVisInfo _info = new JEVisInfo() {

        @Override
        public String getVersion() {
            return "3.0.0";
        }

        @Override
        public String getName() {
            return "JEAPI-SQL";
        }
    };

    /**
     * @deprecated @param db
     * @param port
     * @param schema
     * @param user
     * @param pw
     * @param jevisUser not is use anymore
     * @param jevisPW not is use anymore
     * @throws JEVisException
     */
    public JEVisDataSourceSQL(String db, String port, String schema, String user, String pw, String jevisUser, String jevisPW) throws JEVisException {
        _dbHost = db;
        _dbPort = port;
        _dbUser = user;
        _dbPW = pw;
        _dbSchema = schema;
        _jevisUsername = jevisUser;
        _jevisUserPW = jevisPW;
    }

    /**
     * Enable or disable the use of SSL. Do this befor using the connect()
     * function. SSL is disabled per default.
     *
     * @param enable
     */
    public void enableSSL(boolean enable) {
        ssl = enable;
    }

    /**
     *
     * @param db
     * @param port
     * @param schema
     * @param user
     * @param pw
     * @throws JEVisException
     */
    public JEVisDataSourceSQL(String db, String port, String schema, String user, String pw) throws JEVisException {
        _dbHost = db;
        _dbPort = port;
        _dbUser = user;
        _dbPW = pw;
        _dbSchema = schema;
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

    /**
     *
     * @return
     */
    public boolean connectToDB() {
        try {
            Class.forName("com.mysql.jdbc.Driver");

            String conSring = "jdbc:mysql://" + _dbHost + ":" + _dbPort + "/" + _dbSchema + "?"
                    + "user=" + _dbUser + "&password=" + _dbPW;
            if (ssl) {
                conSring += "&verifyServerCertificate=false&requireSSL=true&useSSL=true";
            }

            logger.info("Using Connection string: {}", conSring);
            DriverManager.setLoginTimeout(20);
            _connect = DriverManager.getConnection(conSring);

            return _connect.isValid(2000);

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
            Benchmark bench = new Benchmark();
            System.out.println("Load all classes");

            getJEVisClasses();
            bench.printBechmark("Loading all classes");

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
//        System.out.println("DS.getJEVisClass: " + name);
        if (SimpleClassCache.getInstance().contains(name)) {
            getClassTable().getObjectClass(name, true);
        }

        return SimpleClassCache.getInstance().getJEVisClass(name);
//        JEVisClass jClass = getClassTable().getObjectClass(name, true);
//        return jClass;
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
//            System.out.println("Build new JEVisClass: " + name);
            if (RelationsManagment.isSysAdmin(_user)) {
//                System.out.println("User is Admin");
                if (getClassTable().insert(name)) {
//                    System.out.println("Insert done");

                    return getClassTable().getObjectClass(name, false);
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

//        List<JEVisRelationship> groups = RelationsManagment.getRelationByType(getCurrentUser(), JEVisConstants.ObjectRelationship.MEMBER_READ);
        Collections.sort(roots);

        return roots;
    }

    private JEVisObject getObjectFromChache(Long id) {
        for (JEVisObject obj : _objectChache) {
            if (obj.getID().equals(id)) {
                return obj;
            }
        }
        return null;
    }

    @Override
    public JEVisObject getObject(Long id) throws JEVisException {
        try {
            JEVisObject obj = null;
            JEVisObject cachedObj = getObjectFromChache(id);
            if (cachedObj != null) {
                return cachedObj;
            }

            obj = getObjectTable().getObject(id, true);
            _objectChache.add(obj);

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
//        System.out.println("getObjects: " + jevisClass);
//        System.out.println("getObject by class: " + jevisClass.getName() + " heirs: " + inherits);

        getCount();
        List<JEVisClass> classes = new ArrayList<JEVisClass>();
        classes.add(jevisClass);
        if (inherits) {
            if (jevisClass.getHeirs() != null) {
                classes.addAll(jevisClass.getHeirs());
            }
        }

        List<JEVisObject> objs = getObjectTable().getObjects(classes);
        List<JEVisObject> returnObjs = new ArrayList<JEVisObject>();

        //TODO: maybe its better to check this in the sql query?
        //Check if the user has permission
        Date startD = new Date();

        List<JEVisObject> userGourps = new ArrayList<JEVisObject>();
        for (JEVisRelationship rel : getCurrentUser().getRelationships(JEVisConstants.ObjectRelationship.MEMBER_READ, JEVisConstants.Direction.FORWARD)) {
//            System.out.println("User is meber in group: " + rel.getOtherObject(getCurrentUser()).getID());
            userGourps.add(rel.getOtherObject(getCurrentUser()));
        }

        for (JEVisObject obj : objs) {
            for (JEVisRelationship rel : obj.getRelationships(JEVisConstants.ObjectRelationship.OWNER, JEVisConstants.Direction.FORWARD)) {
//                System.out.println("GetObject.rel: " + rel);
                for (JEVisObject group : userGourps) {
//                    System.out.println("Compare: " + group.getID() + " to " + rel.getEndObject().getID());
                    if (group.getID().equals(rel.getEndObject().getID())) {
//                        System.out.println("Is readable: " + rel);
                        if (!returnObjs.contains(rel.getStartObject())) {
                            returnObjs.add(obj);
                        }

                    } else {
//                        System.out.println("--->knot");
                    }
                }

            }
        }
        Date nowD = new Date();
        System.out.println("time for userright check: " + (nowD.getTime() - startD.getTime()) + " ms and " + getCount());

//        Collections.sort(objs);
        Collections.sort(returnObjs);

        return returnObjs;
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

    @Override
    public JEVisInfo getInfo() {
        return _info;
    }

    @Override
    public boolean isConnectionAlive() throws JEVisException {
        if (_connect != null) {
            try {
                if (_connect.isValid(10)) {
                    return true;
                } else {
                    return false;
                }
            } catch (SQLException ex) {
                java.util.logging.Logger.getLogger(JEVisDataSourceSQL.class.getName()).log(Level.SEVERE, null, ex);
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean reconnect() throws JEVisException {
        System.out.println("Reconnect with :" + _jevisUsername + " // " + _jevisUserPW);
        if (connectToDB()) {
            return connect(_jevisUsername, _jevisUserPW);
        } else {
            return false;
        }

    }

    @Override
    public boolean disconnect() throws JEVisException {
        try {
            _connect.close();
            return true;
        } catch (SQLException ex) {
            java.util.logging.Logger.getLogger(JEVisDataSourceSQL.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
    }

    @Override
    public List<JEVisUnit> getUnits() {
        List<JEVisUnit> units = new ArrayList<JEVisUnit>();

        units.add(new JEVisUnitImp("(1000*W)*h", "kWh", JEVisUnit.Prefix.NONE));
//        units.add(new JEVisUnitImp("(#*W)*h", "#Wh"));

        return units;
    }

}
