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

import jakarta.persistence.MappedSuperclass;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

@MappedSuperclass
public abstract class MovieBase {
    public enum Rating {G, PG, PG13, R}

    @JacksonXmlProperty(isAttribute = true)
    int rank;

    @JacksonXmlProperty(isAttribute = true)
    String tit;

    @JacksonXmlProperty(isAttribute = true)
    double grs;

    @JacksonXmlProperty(isAttribute = true)
    Rating rating;

    public int getRank() {
        return rank;
    }

    public void setRank(final int rank) {
        this.rank = rank;
    }

    public String getTit() {
        return tit;
    }

    public void setTit(final String tit) {
        this.tit = tit;
    }

    public double getGrs() {
        return grs;
    }

    public void setGrs(final double grs) {
        this.grs = grs;
    }

    public Rating getRating() {
        return rating;
    }

    public void setRating(final Rating rating) {
        this.rating = rating;
    }
}
