/**
 * Copyright (C) 2013 - 2015 Envidatec GmbH <info@envidatec.com>
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import org.jevis.api.JEVisOption;
import org.jevis.api.JEVisRelationship;
import org.jevis.api.JEVisUnit;
import org.jevis.commons.JEVisUser;
import org.jevis.commons.config.CommonOptions;
import org.jevis.commons.unit.JEVisUnitImp;
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
    private TypeTable _tt;

    private int qCount = 0;//for benchmarking
    private List<String> _qNames = new ArrayList<String>();
    private boolean ssl = false;
    private JEVisUser _userObject;

    //workaround to keep the information that all classes are allready loaded
    private boolean _allClassesLoaded = false;

    private List<JEVisOption> _configuration = new ArrayList<JEVisOption>();

    final private JEVisInfo _info = new JEVisInfo() {

        @Override
        public String getVersion() {
            return "3.0.7";
        }

        @Override
        public String getName() {
            return "JEAPI-SQL";
        }
    };

    /**
     * Enable or disable the use of SSL. Do this befor using the connect()
     * function. SSL is disabled per default.
     *
     * @param enable
     */
    public void enableSSL(boolean enable) {
        ssl = enable;
    }

    public JEVisDataSourceSQL() {
    }

    @Override
    public void init(List<JEVisOption> config) throws IllegalArgumentException {
        setConfiguration(config);
    }

    @Override
    public void setConfiguration(List<JEVisOption> config) {
        for (JEVisOption opt : config) {
            if (opt.getKey().equals(CommonOptions.DataSource.DataSource.getKey())) {
                _dbHost = opt.getOption(CommonOptions.DataSource.HOST.getKey()).getValue();
                _dbPort = opt.getOption(CommonOptions.DataSource.PORT.getKey()).getValue();
                _dbSchema = opt.getOption(CommonOptions.DataSource.SCHEMA.getKey()).getValue();
                _dbUser = opt.getOption(CommonOptions.DataSource.USERNAME.getKey()).getValue();
                _dbPW = opt.getOption(CommonOptions.DataSource.PASSWORD.getKey()).getValue();
            }
        }

    }

    @Override
    public List<JEVisOption> getConfiguration() {
        if (_configuration.isEmpty()) {
            _configuration.add(CommonOptions.DataSource.DataSource);
        }

        return _configuration;
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
//        System.out.println("JEVisDataSourceSQL.connect: dbHost: " + _dbHost + " dbPort: " + _dbPort + " dbSchema: " + _dbSchema + " dbUser: " + _dbUser + "dbPW: *****");
        _jevisUsername = username;
        _jevisUserPW = password;
        SimpleClassCache.getInstance().setDataSource(this);

        if (connectDB()) {
//            System.out.println("DB connection is OK, login user");
            loginUser();//throw exeption is something is wrong
            return true;
        } else {
            throw new JEVisException("Error DataSource is not connected ", 2134);
        }
    }

    public Connection getConnection() throws JEVisException {
        return _connect;
    }

    @Override
    public JEVisObject getCurrentUser() throws JEVisException {
        return _user;
    }

    public JEVisUser getCurrentUserObject() throws JEVisException {
        return _userObject;
    }

    public void setCurrentUserObject(JEVisUser user) {
        _userObject = user;
    }

    /**
     *
     * @return
     */
    public boolean connectDB() {
        try {
            logger.debug("connectDB: host: " + _dbHost + " port: " + _dbPort + " schema: " + _dbSchema + " user: " + _dbUser + " pw: " + _dbPW);
            ConnectionFactory.getInstance().registerMySQLDriver(_dbHost, _dbPort, _dbSchema, _dbUser, _dbPW);

            _connect = ConnectionFactory.getInstance().getConnection();

            return _connect.isValid(2000);

        } catch (Exception ex) {
            ex.printStackTrace();
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
//            Benchmark bench = new Benchmark();
//            System.out.println("Load all classes");

//            getJEVisClasses();
//            bench.printBechmark("Loading all classes");
            _user = getObjectTable().loginUser(this, _jevisUsername, _jevisUserPW);
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
        if (!SimpleClassCache.getInstance().contains(name)) {
            JEVisClass jclass = getClassTable().getObjectClass(name, false);

        }

        return SimpleClassCache.getInstance().getJEVisClass(name);
//        JEVisClass jClass = getClassTable().getObjectClass(name, true);
//        return jClass;
    }

    @Override
    public List<JEVisClass> getJEVisClasses() throws JEVisException {
        try {

            if (_allClassesLoaded) {
                return SimpleClassCache.getInstance().getAllClasses();
            } else {
                List<JEVisClass> jClass = getClassTable().getAllObjectClasses();
                Collections.sort(jClass);
                _allClassesLoaded = true;
                return jClass;
            }

        } catch (SQLException ex) {
            throw new JEVisException("error while select object class from JEVis MySQL Datsource  ", JEVisExceptionCodes.DATASOURCE_FAILD, ex);
        }
    }

    @Override
    public JEVisClass buildClass(String name) throws JEVisException {
        try {
//            System.out.println("Build new JEVisClass: " + name);
//            System.out.println("is sysadmin: " + RelationsManagment.isSysAdmin(_user));
            if (RelationsManagment.isSysAdmin(_user)) {
//                System.out.println("User is Admin");
                if (getClassTable().insert(name)) {
//                    System.out.println("Insert done");
//
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
                if (!roots.contains(obj)) {
                    roots.add(obj);
                }

            }
        }

        for (JEVisRelationship rel : RelationsManagment.getRelationByType(_user, JEVisConstants.ObjectRelationship.ROOT)) {
            if (!roots.contains(rel.getOtherObject(getCurrentUser()))) {
                roots.add(rel.getOtherObject(getCurrentUser()));
            }

        }

        Collections.sort(roots);

        return roots;
    }

    @Override
    public JEVisObject getObject(Long id) throws JEVisException {
        try {
            JEVisObject obj = null;
            if (SimpleObjectCache.getInstance().contains(id)) {
                obj = SimpleObjectCache.getInstance().getObject(id);
            } else {
                obj = getObjectTable().getObject(id, true);
            }

//            _objectChache.add(obj);
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
        List<JEVisObject> returnObjs = new ArrayList<JEVisObject>();

//        getCount();
        List<JEVisClass> classes = new ArrayList<JEVisClass>();
        classes.add(jevisClass);
        if (inherits) {
            if (jevisClass.getHeirs() != null) {
                classes.addAll(jevisClass.getHeirs());
            }
        }

        List<JEVisObject> objs = getObjectTable().getObjects(classes);
//        System.out.println("Total Object count: " + objs.size());

        //TODO: maybe its better to check this in the sql query?
        //Check if the user has permission
//        Date startD = new Date();
        if (getCurrentUserObject().isSysAdmin()) {//Sys Admins ignore userrights
            returnObjs.addAll(objs);
        } else {
            List<JEVisObject> userGourps = new ArrayList<JEVisObject>();
            for (JEVisRelationship rel : getCurrentUser().getRelationships(JEVisConstants.ObjectRelationship.MEMBER_READ, JEVisConstants.Direction.FORWARD)) {
//            System.out.println("User is meber in group: " + rel.getOtherObject(getCurrentUser()).getID());
                userGourps.add(rel.getOtherObject(getCurrentUser()));
            }

            for (JEVisObject obj : objs) {
//            System.out.print("o:" + obj.getName());
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
        }
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

    protected TypeTable getTypeTable() throws JEVisException {
        if (_tt == null) {
            _tt = new TypeTable(this);
        }
        return _tt;
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

    protected void addQuery(String name) {
        qCount++;

        //disabled
//        _qNames.add(name);
    }

    public int getCount() {
        int tmp = qCount;
        qCount = 0;
        _qNames = new ArrayList<String>();

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
        if (connectDB()) {
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
