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
package org.jevis.jeapi.sql;

import java.util.ArrayList;
import java.util.List;
import org.jevis.jeapi.JEVisAttribute;
import org.jevis.jeapi.JEVisClass;
import org.jevis.jeapi.JEVisClassRelationship;
import org.jevis.jeapi.JEVisObject;
import org.jevis.jeapi.JEVisRelationship;
import static org.jevis.jeapi.JEVisConstants.ObjectRelationship.*;
import static org.jevis.jeapi.JEVisConstants.Class.*;
import static org.jevis.jeapi.JEVisConstants.Attribute.*;
import static org.jevis.jeapi.JEVisConstants.ClassRelationship.*;
import org.jevis.jeapi.JEVisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Florian Simon <florian.simon@envidatec.com>
 */
public class RelationsManagment {

    static Logger logger = LoggerFactory.getLogger(JEVisDataSourceSQL.class);

    /**
     *
     * @param user
     * @param object
     * @return
     */
    public static boolean canRead(JEVisObject user, JEVisObject object) throws JEVisException {
        return checkMebershipForType(user, object, MEMBER_READ);
    }

    /**
     *
     * @param user
     * @param object
     * @return
     */
    public static boolean canWrite(JEVisObject user, JEVisObject object) throws JEVisException {
        return checkMebershipForType(user, object, MEMBER_WRITE);
    }

    /**
     *
     * @param user
     * @param object
     * @return
     */
    public static boolean canDelete(JEVisObject user, JEVisObject object) throws JEVisException {
        return checkMebershipForType(user, object, MEMBER_DELETE);
    }

    /**
     *
     * @param user
     * @param object
     * @return
     */
    public static boolean canCreate(JEVisObject user, JEVisObject object) throws JEVisException {
        return checkMebershipForType(user, object, MEMBER_CREATE);
    }

    /**
     *
     * @param user
     * @param object
     * @return
     */
    public static boolean canExcecude(JEVisObject user, JEVisObject object) throws JEVisException {
        return checkMebershipForType(user, object, MEMBER_EXCECUTE);
    }

    /**
     *
     * @param user
     * @param object
     * @param type
     * @return
     */
    public static boolean checkMebershipForType(JEVisObject user, JEVisObject object, int type) throws JEVisException {
//        List<JEVisRelationship> objMemberships = object.getr(object);
        List<JEVisRelationship> userMemberships = getMembershipsRel(user);
        logger.debug("Mebership size: object[{}] user[{}]", object.getRelationships().size(), userMemberships.size());

        for (JEVisRelationship or : object.getRelationships()) {
            for (JEVisRelationship ur : userMemberships) {
                //is the Object Owner end the same as the user membership end
                logger.debug("object.owner[{}]==user.membership[{}]", ur.getEndObject().getID(), or.getEndObject().getID());
                if (ur.getEndObject().getID() == or.getEndObject().getID()) {
                    logger.debug("true");
                    if (ur.isType(type)) {
                        return true;
                    }

                }
            }
        }

        return false;
    }

    /**
     *
     * @param object
     * @return
     */
    public static List<JEVisRelationship> getRelationByType(JEVisObject object, int type) throws JEVisException {
        List<JEVisRelationship> memberships = new ArrayList<JEVisRelationship>();
        List<JEVisRelationship> objRel = object.getRelationships();
        logger.debug("Relationship.size: {}", objRel.size());
        for (JEVisRelationship r : objRel) {
            if (r.isType(type)) {
                memberships.add(r);
            }
        }
        return memberships;
    }

    /**
     *
     * @param object
     * @return
     */
    public static List<JEVisRelationship> getMembershipsRel(JEVisObject object) throws JEVisException {
        logger.debug("getMembershipsRelations for {}", object.getID());
        List<JEVisRelationship> memberships = new ArrayList<JEVisRelationship>();
        List<JEVisRelationship> objRel = object.getRelationships();
        logger.debug("Relationship totals: {}", objRel.size());

        for (JEVisRelationship r : objRel) {
            logger.debug("Checking relationship: {}->{} [{}]", r.getStartObject().getID(), r.getEndObject().getID(), r.getType());
            if (r.isType(MEMBER_READ)
                    || r.isType(MEMBER_WRITE)
                    || r.isType(MEMBER_EXCECUTE)
                    || r.isType(MEMBER_CREATE)
                    || r.isType(MEMBER_DELETE)) {
                logger.debug("Found membership: {}", r);
                memberships.add(r);
            }
        }
        logger.debug("done searching");
        return memberships;
    }

    public static boolean isSysAdmin(JEVisObject user) throws JEVisException {
        if (user.getJEVisClass().getName().equals(USER)) {
            JEVisAttribute sysAdmin = user.getAttribute(USER_SYS_ADMIN);
            return sysAdmin.getLatestSample().getValueAsBoolean();
        }
        return false;
    }

    public static boolean isParentRelationship(JEVisClass parent, JEVisClass child) throws JEVisException {

        if (child.getInheritance() != null) {
            return isParentRelationship(parent, child.getInheritance());
        }

        for (JEVisClassRelationship rel : parent.getRelationships(OK_PARENT)) {
            if (rel.getOtherClass(parent).equals(child)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isNestedRelationship(JEVisClass parent, JEVisClass child) throws JEVisException {
        if (child.getInheritance() != null) {
            return isNestedRelationship(parent, child.getInheritance());
        }

        for (JEVisClassRelationship rel : parent.getRelationships(NESTEDT)) {
            if (rel.getOtherClass(parent).equals(child)) {
                return true;
            }
        }

        return false;
    }
}
