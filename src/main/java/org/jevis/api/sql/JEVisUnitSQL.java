/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.api.sql;

import javax.measure.unit.BaseUnit;
import javax.measure.unit.Unit;
import org.jevis.api.JEVisUnit;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class JEVisUnitSQL implements JEVisUnit {

    private Unit _unit;

    public JEVisUnitSQL(Unit _unit) {
        this._unit = _unit;
    }

    public JEVisUnitSQL(String unit) {
        if (unit == null || unit.isEmpty()) {
            System.out.println("unit is null");
            _unit = Unit.ONE;
        } else {
            System.out.println("unit is not null");
            _unit = Unit.valueOf(unit);
        }

    }

    @Override
    public boolean isSIUnit() {
        return _unit.isStandardUnit();
    }

    @Override
    public JEVisUnit getSIUnit() {
        return new JEVisUnitSQL(_unit.getStandardUnit());
    }

    @Override
    public String getSymbol() {
        return _unit.toString();//not save??
    }

// belongs to the math part of the api !?
//    public void ableitung(boolean intensiv, double x, double y) {
//        if (intensiv) {
//        } else {
//        }
//
//    }
    @Override
    public boolean equals(JEVisUnit unit) {
        //TODo: find a fast solution then to parse it again and agin.....
        BaseUnit otherUnit = new BaseUnit(unit.getSymbol());
        return _unit.equals(otherUnit);
    }

    @Override
    public boolean isCompatible(JEVisUnit unit) {
        BaseUnit otherUnit = new BaseUnit(unit.getSymbol());
        return _unit.isCompatible(otherUnit);
    }

    public Unit getUnit() {
        return _unit;
    }
}
