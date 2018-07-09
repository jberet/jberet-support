/*
 * Copyright (c) 2015 Red Hat, Inc. and/or its affiliates.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */
 
package org.jberet.support.io;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

public class Person2 extends Person {

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "M/D/yyyy")
    Date birthday;

    @Override
    public Date getBirthday() {
        return birthday;
    }

    @Override
    public void setBirthday(final Date birthday) {
        this.birthday = birthday;
    }

}
