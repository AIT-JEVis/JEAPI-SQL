/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.jeapi.sql;

import org.jevis.jeapi.JEVisUnit;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisUnitSQL implements JEVisUnit{

    private String _name;
    
    public JEVisUnitSQL(JEVisDataSourceSQL ds,String name) {
        _name=name;
    }


    public boolean isSIUnit() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public JEVisUnit getSIUnit() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public String getName() {
        return _name;
    }

    public void doMagic() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void setName(String name) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}
