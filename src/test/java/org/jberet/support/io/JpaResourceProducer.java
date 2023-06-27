/*
 * Copyright (c) 2016 Red Hat, Inc. and/or its affiliates.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.jberet.support.io;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;

import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;

@Named
public class JpaResourceProducer {
    static EntityManagerFactory emf;
    static EntityManager em;

    @Produces
    public EntityManager getEntityManager() {
        return em;
    }

}
