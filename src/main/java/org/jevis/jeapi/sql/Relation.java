/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.jeapi.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisException;
import org.jevis.jeapi.JEVisExceptionCodes;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class Relation {

    String _jclass;
    String _iclass;
    String _okclass;
    private JEVisClass _class;
    

    public Relation(JEVisDataSourceSQL ds,ResultSet rs,JEVisClass jclass) throws JEVisException {
        try {
            _class=jclass;
            _jclass = rs.getString(ClassRelationTable.COLUMN_CLASS);
            _okclass = rs.getString(ClassRelationTable.COLUMN_OKPARENT);

        } catch (SQLException ex) {
            throw new JEVisException("Cannot parse Object", JEVisExceptionCodes.DATASOURCE_FAILD_MYSQL, ex);
        }
    }
    
    
    
    public Relation(String jclass, String iclass, String okclass) {
        _jclass=jclass;
        _iclass=iclass;
        _okclass=okclass;
    }
    
    
    public boolean isValidParent(){
        return _okclass!=null && !_okclass.isEmpty();
    }

    public String getJEVisClass() {
        return _jclass;
    }

    public void setJEVisClass(String jclass) {
        this._jclass = jclass;
    }


    public String getValidParent() {
        return _okclass;
    }

    public void setValidParent(String okclass) {
        this._okclass = okclass;
    }
    
    
    
    
    
}
