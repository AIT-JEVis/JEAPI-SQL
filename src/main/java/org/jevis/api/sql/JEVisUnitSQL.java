/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.api.sql;

import java.util.ArrayList;
import java.util.List;
import javax.measure.unit.Unit;
import org.jevis.api.JEVisUnit;
import org.jevis.api.JEVisUnitRelationship;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisUnitSQL implements JEVisUnit {

    private List<JEVisUnitRelationship> fakeRelaionships = new ArrayList<JEVisUnitRelationship>();
    private Unit _unit;

    public JEVisUnitSQL(Type type, JEVisUnitRelationship.Type relType, String unitA, String unitB) {
        JEVisUnitRelationship rel = new JEVisUnitRealtionshipSQL(relType, unitA, unitB);

        buildRel(relType, unitA, unitB);

        fakeRelaionships.add(rel);
    }

    public JEVisUnitSQL(Type type, JEVisUnit origianl, String alterativName) {
        JEVisUnitRelationship rel = new JEVisUnitRealtionshipSQL(JEVisUnitRelationship.Type.ALTERNATE, origianl.getSymbol(), alterativName);

        fakeRelaionships.add(rel);
    }

    private void buildRel(JEVisUnitRelationship.Type relType, String unitA, String unitB) {

        Unit newUnit = Unit.ONE;

        switch (relType) {
            case DIVIDE:
                newUnit = Unit.valueOf(unitA).divide(Unit.valueOf(unitB));
                break;
            case TIMES:
                newUnit = Unit.valueOf(unitA).times(Unit.valueOf(unitB));
                break;
        }

        System.out.println("New Unit: " + newUnit);

        _unit = newUnit;

    }

    public JEVisUnitSQL(Unit _unit) {
        this._unit = _unit;
    }

    @Override
    public JEVisUnit parseUnit(String unit) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JEVisUnit parseUnit(Unit unit) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public JEVisUnit getSIUnit() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public String getSymbol() {
        return _unit.toString();
    }

    @Override
    public String getLabel() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Type getType() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<JEVisUnitRelationship> getRelationships() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean equals(JEVisUnit unit) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isCompatible(JEVisUnit unit) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
