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
package io.mycat.exporter;

import io.prometheus.client.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CollectorList extends Collector {

    final List<Collector> collectors;

    public CollectorList(Collector... collectors) {
        this.collectors = Arrays.asList(collectors);
    }

    public CollectorList(List<Collector> collectors) {
        this.collectors = collectors;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return collectors.stream().flatMap(i -> i.collect().stream()).collect(Collectors.toList());
    }
}