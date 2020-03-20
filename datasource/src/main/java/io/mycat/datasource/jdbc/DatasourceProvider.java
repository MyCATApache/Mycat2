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
package io.mycat.datasource.jdbc;

import io.mycat.config.DatasourceRootConfig;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;

import javax.transaction.*;

/**
 * @author Junwen Chen
 **/
public interface DatasourceProvider {

    JdbcDataSource createDataSource(DatasourceRootConfig.DatasourceConfig dataSource);

    void closeDataSource(JdbcDataSource dataSource);

    default UserTransaction createUserTransaction() {
        return NONE;
    }

    static final UserTransaction NONE = new UserTransaction() {

        @Override
        public void begin() throws NotSupportedException, SystemException {

        }

        @Override
        public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {

        }

        @Override
        public void rollback() throws IllegalStateException, SecurityException, SystemException {

        }

        @Override
        public void setRollbackOnly() throws IllegalStateException, SystemException {

        }

        @Override
        public int getStatus() throws SystemException {
            return 0;
        }

        @Override
        public void setTransactionTimeout(int seconds) throws SystemException {

        }
    };
}
