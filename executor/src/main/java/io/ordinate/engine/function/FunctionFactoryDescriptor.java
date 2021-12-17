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

import io.ordinate.engine.schema.InnerType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Getter
@EqualsAndHashCode
public class FunctionFactoryDescriptor {
    final String name;
    final List<InnerType> argTypes;
    private FunctionFactory factory;
    private InnerType type;

    public FunctionFactoryDescriptor(FunctionFactory factory) {
        this.factory = factory;
        String signature = Objects.requireNonNull(factory.getSignature(), factory.getClass().getCanonicalName());
        int startIndex = signature.indexOf("(") + 1;
        int endIndex = signature.lastIndexOf(")");
        name = signature.substring(0, startIndex - 1);
        String argsText = signature.substring(startIndex, endIndex);
        List<InnerType> args = Arrays.stream(argsText.split(",")).filter(i->!i.isEmpty()).map(i -> getArgType(i.trim())).collect(Collectors.toList());
        argTypes = args.stream().map(t -> getArgType(t.getAlias())).collect(Collectors.toList());
        int tailEndIndex = signature.lastIndexOf(":");
        type = tailEndIndex != -1 ? getArgType(signature.substring(tailEndIndex + 1)) : null;
    }

    public InnerType getArgType(String argType) {
        if (argType.equals("int")){
            argType = "int32";
        }
        if (argType.equals("short")){
            argType = "int16";
        }
        if (argType.equals("long")){
            argType = "int64";
        }
        if (argType.equals("byte")){
            argType = "int8";
        }
        for (InnerType value : InnerType.values()) {
            if (value.getAlias().equalsIgnoreCase(argType)) {
                return value;
            }
        }
        throw new UnsupportedOperationException();
    }
}
