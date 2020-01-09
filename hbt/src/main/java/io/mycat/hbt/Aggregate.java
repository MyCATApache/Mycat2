/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.hbt;

/**
 * @author jamie12221
 **/
public enum Aggregate {
    COUNT_STAR,
    COUNT,
    MIN,
    MAX,
    LAST_VALUE,
    ANY_VALUE,
    FIRST_VALUE,
    NTH_VALUE,
    LEAD,
    LAG,
    NTILE,
    SINGLE_VALUE,
    AVG,
    STDDEV_POP,
    REGR_COUNT,
    REGR_SXX,
    REGR_SYY,
    COVAR_POP,
    COVAR_SAMP,
    STDDEV_SAMP,
    STDDEV,
    VAR_POP,
    VAR_SAMP,
    VARIANCE,
    BIT_AND,
    BIT_OR,
}