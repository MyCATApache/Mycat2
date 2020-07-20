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
package io.mycat.hbt3;

import org.apache.calcite.rel.RelNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

public class NextConvertor {
    final IdentityHashMap<Class, Set<Class>> map = new IdentityHashMap<Class, Set<Class>>();

    public void put(Class key, Class... values) {
        Set<Class> set = Collections.newSetFromMap(new IdentityHashMap<>());
        map.put(key, set);
        set.addAll(Arrays.asList(values));
    }

    public boolean check(RelNode input, Class<?> up) {
        if (input instanceof View) {
            input = ((View) input).getRelNode();
        }
        return innerCheck( input.getClass(), up);
    }

    public boolean innerCheck(Class  inputClass, Class<?> up) {
        Set<Class> classes = map.get(inputClass);
        if (classes == null) {
            Class need = null;
            for (Class aClass : map.keySet()) {
                if (aClass == inputClass || inputClass.isAssignableFrom(aClass) || aClass.isAssignableFrom(inputClass)) {
                    need = aClass;
                    break;
                }
            }
            if (need == null) {
                return false;
            } else {
                classes = map.get(need);
            }
        }
        for (Class aClass : classes) {
            if (aClass == up || up.isAssignableFrom(aClass) || aClass.isAssignableFrom(up)) {
                return true;
            }
        }
        return false;
    }
}