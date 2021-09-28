/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.function;

import org.apache.calcite.util.NameMap;

import java.util.*;

public class FunctionFactoryCache {
    final NameMap<ArrayList<FunctionFactoryDescriptor>> map = new NameMap<>();

    public FunctionFactoryCache() {
        Iterable<FunctionFactory> functionFactories = ServiceLoader.load(FunctionFactory.class, FunctionFactory.class.getClassLoader());
        for (FunctionFactory functionFactory : functionFactories) {
            try {
                FunctionFactoryDescriptor functionFactoryDescriptor = new FunctionFactoryDescriptor(functionFactory);
                ArrayList<FunctionFactoryDescriptor> list;
                String name = functionFactoryDescriptor.getName();
                if (!map.containsKey(name, false)) {
                    list = new ArrayList<>();
                    list.add(functionFactoryDescriptor);
                    map.put(name, list);
                } else {
                    NavigableMap<String, ArrayList<FunctionFactoryDescriptor>> range = map.range(name, false);
                    range.get(name).add(functionFactoryDescriptor);
                }
            } catch (Exception exception) {
                exception.fillInStackTrace();
            }

        }
    }

    public List<FunctionFactoryDescriptor> getListByName(String name) {
        NavigableMap<String, ArrayList<FunctionFactoryDescriptor>> range = map.range(name, false);
        if (range.isEmpty()) return Collections.emptyList();
        return range.get(name);
    }

    public Optional<FunctionFactoryDescriptor> getListFullName(String name) {
        List<FunctionFactoryDescriptor> factoryDescriptors = getListByName(name.substring(0, name.indexOf('(') ));
        if (factoryDescriptors.isEmpty()) {
            return Optional.empty();
        }
        return factoryDescriptors.stream().filter(f -> f.getFactory().getSignature().equalsIgnoreCase(name)).findFirst();
    }
}
