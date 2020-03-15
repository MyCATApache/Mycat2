/**
 * Copyright (C) <2019>  <chen junwen>
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

package io.mycat.client;

import io.mycat.MycatDataContext;
import io.mycat.beans.mycat.TransactionType;

/**
 * @author Junwen Chen
 **/
public interface MycatClient extends MycatDataContext {
    public Context analysis(String sql) ;
    public void useSchema(String schemaName);
    public TransactionType getTransactionType();
    public void useTransactionType(TransactionType transactionType);
    public String getDefaultSchema();

    public <T> T getMycatDb();
}