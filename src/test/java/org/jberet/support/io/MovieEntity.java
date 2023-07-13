/*
 * Copyright (c) 2014-2015 Red Hat, Inc. and/or its affiliates.
 *
 * This program and the accompanying materials are made
 * available under the terms of the Eclipse Public License 2.0
 * which is available at https://www.eclipse.org/legal/epl-2.0/
 *
 * SPDX-License-Identifier: EPL-2.0
 */

package org.jberet.support.io;

import java.io.Serializable;
import java.util.Date;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import jakarta.persistence.AccessType;
import jakarta.persistence.Basic;
import jakarta.persistence.Entity;

@Entity
@jakarta.persistence.Access(AccessType.FIELD)
public class MovieEntity extends MovieBase implements Serializable {
    private static final long serialVersionUID = -8771060045002998154L;

    @jakarta.persistence.Id
    @jakarta.persistence.GeneratedValue
    long id;

    @Basic
    @JacksonXmlProperty(isAttribute = true)
    private Date opn;

    public long getId() {
        return id;
    }

    public void setId(final long id) {
        this.id = id;
    }

    public Date getOpn() {
        return opn;
    }

    public void setOpn(final Date opn) {
        this.opn = opn;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Movie{");
        sb.append("id='").append(id).append('\'');
        sb.append(", rank=").append(rank);
        sb.append(", tit='").append(tit).append('\'');
        sb.append(", grs=").append(grs);
        sb.append(", opn=").append(opn);
        sb.append(", rating=").append(rating);
        sb.append('}');
        return sb.toString();
    }
}
