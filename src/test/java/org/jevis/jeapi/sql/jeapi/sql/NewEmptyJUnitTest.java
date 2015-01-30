/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jevis.jeapi.sql.jeapi.sql;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.jevis.api.JEVisUnit;
import org.jevis.api.JEVisUnitRelationship;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class NewEmptyJUnitTest extends TestCase {

    public NewEmptyJUnitTest(String testName) {
        super(testName);

    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void test_Units() {
//        JEVisUnitSQL wattHours = new JEVisUnitSQL(JEVisUnit.Type.BASE, JEVisUnitRelationship.Type.TIMES, "W", "h");
//        Assert.assertEquals(wattHours.getSymbol(), "W·h");

    }

    // TODO add test methods here. The name must begin with 'test'. For example:
    // public void testHello() {}
}
