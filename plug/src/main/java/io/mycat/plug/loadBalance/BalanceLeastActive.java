/**
 * Copyright (C) <2021>  <chen junwen>
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

package io.mycat.plug.loadBalance;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 最小连接数优先
 */
public enum BalanceLeastActive implements LoadBalanceStrategy {
    INSTANCE {
        @Override
        public LoadBalanceElement select(LoadBalanceInfo info, List<LoadBalanceElement> entityList) {
            if (null == entityList || entityList.isEmpty()) {
                return null;
            }
            int len = entityList.size();
            List<LoadBalanceElement> balanceList = new ArrayList<>();

            int leastActive = Integer.MAX_VALUE;
            for (int i = 0; i < len; i++) {
                LoadBalanceElement le = entityList.get(i);
                if (le == null) {
                    continue;
                }
                if (leastActive > le.getSessionCounter()) {
                    leastActive = le.getSessionCounter();
                    balanceList.clear();
                    balanceList.add(le);
                } else if (leastActive == le.getSessionCounter()) {
                    balanceList.add(le);
                }
            }
            int size = balanceList.size();
            if (1 == size) {
                return balanceList.get(0);
            }
            int i = ThreadLocalRandom.current().nextInt(0, size);
            return balanceList.get(i);
        }
    }
}

