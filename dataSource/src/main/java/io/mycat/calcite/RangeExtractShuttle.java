package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;

import java.util.*;

public class RangeExtractShuttle extends RexShuttle {
    private final Map<Integer, RangeSet<Comparable<?>>> operandRanges;
    private final RangeSetColumnProvider provider;
    private final RexSimplify rexSimplify;


    public interface RangeSetColumnProvider {
        RangeSet getAllByIndex(int index);

    }

    public RangeExtractShuttle(Map<Integer, RangeSet<Comparable<?>>> operandRanges, RangeSetColumnProvider provider, RexSimplify rexSimplify) {
        this.operandRanges = operandRanges;
        this.provider = provider;
        this.rexSimplify = rexSimplify;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        if (!operandRanges.containsKey(inputRef.getIndex())){
            operandRanges.put(inputRef.getIndex(),provider.getAllByIndex(inputRef.getIndex()));
        }
        return super.visitInputRef(inputRef);
    }

    @Override
    public RexNode visitCall(RexCall call) {
        if (call.getOperands().size() == 2) {
            RexNode op0 = call.operands.get(0);
            RexNode op1 = call.operands.get(1);
            if (op0.getKind() == SqlKind.LITERAL) {
                if (op1.getKind() == SqlKind.INPUT_REF) {
                    RexNode tmp = op0;
                    op0 = op1;
                    op1 = tmp;
                }
            }
            if (op0.getKind() == SqlKind.INPUT_REF) {
                if (op1.getKind() == SqlKind.LITERAL) {
                    RexInputRef inputRef = (RexInputRef) op0;
                    RexLiteral literal = (RexLiteral) op1;
                    Comparable value = literal.getValue();
                    RangeSet<Comparable<?>> comparableRangeSet = provider.getAllByIndex(inputRef.getIndex());
                    switch (call.getKind()) {
                        case EQUALS:
                            comparableRangeSet = comparableRangeSet.subRangeSet(Range.singleton(value));
                            break;
                        case GREATER_THAN_OR_EQUAL:
                            comparableRangeSet = comparableRangeSet.subRangeSet(Range.atLeast(value));
                            break;
                        case LESS_THAN_OR_EQUAL:
                            comparableRangeSet = comparableRangeSet.subRangeSet(Range.atMost(value));
                            break;
                        case GREATER_THAN:
                            comparableRangeSet = comparableRangeSet.subRangeSet(Range.greaterThan(value));
                            break;
                        case LESS_THAN: {
                            comparableRangeSet = comparableRangeSet.subRangeSet(Range.lessThan(value));
                            break;
                        }
                        case NOT_EQUALS: {
                            comparableRangeSet = comparableRangeSet.subRangeSet(Range.singleton(value));
                            break;
                        }
                        // fall through
                        default:
                            break;
                    }
                    operandRanges.put(inputRef.getIndex(), comparableRangeSet);
                }
            }
        }
        List<RexNode> operands = call.getOperands();
        List<Map<Integer, RangeSet<Comparable<?>>>> res = new ArrayList<>();
        for (RexNode operand : operands) {
            Map<Integer, RangeSet<Comparable<?>>> subRanges = new HashMap<>();
            RexNode simplify = rexSimplify.simplify(operand);
            simplify.accept(new RangeExtractShuttle(subRanges, provider,rexSimplify));
            res.add(subRanges);
        }
        switch (call.getKind()) {
            case NOT: {
                if (res.size() != 1) {
                    throw new IllegalArgumentException();
                } else {
                    operandRanges.putAll(res.get(0));
                }
                break;
            }
            case AND: {
                Optional<Map<Integer, RangeSet<Comparable<?>>>> reduce = res.stream().reduce((one, two) -> {
                    for (Map.Entry<Integer, RangeSet<Comparable<?>>> t : two.entrySet()) {
                        one.merge(t.getKey(), t.getValue(), (comparableRangeSet, comparableRangeSet2) -> {
                            TreeRangeSet<Comparable<?>> set = TreeRangeSet.create(comparableRangeSet);
                            return set.subRangeSet(comparableRangeSet2.span());
                        });
                    }
                    return one;
                });
                reduce.ifPresent(m -> operandRanges.putAll(m));
                break;
            }
            case OR: {
                Optional<Map<Integer, RangeSet<Comparable<?>>>> reduce = res.stream().reduce((one, two) -> {
                    for (Map.Entry<Integer, RangeSet<Comparable<?>>> t : two.entrySet()) {
                        one.merge(t.getKey(), t.getValue(), (comparableRangeSet, comparableRangeSet2) -> {
                            TreeRangeSet<Comparable<?>> set = TreeRangeSet.create(comparableRangeSet);
                            if (comparableRangeSet2.isEmpty()){
                                comparableRangeSet2 = provider.getAllByIndex(t.getKey());
                            }
                            set.addAll(comparableRangeSet2);
                            return set;
                        });
                    }
                    return one;
                });
                reduce.ifPresent(i -> operandRanges.putAll(i));
                break;
            }
        }
        return super.visitCall(call);
    }

    private boolean isExtractCall(RexNode op1) {
        return false;
    }

    private boolean canRewriteExtract(RexNode operand) {
//        // We rely on timeUnits being sorted (so YEAR comes before MONTH
//        // before HOUR) and unique. If we have seen a predicate on YEAR,
//        // operandRanges will not be empty. This checks whether we can rewrite
//        // the "extract" condition. For example, in the condition
//        //
//        //   extract(MONTH from time) = someValue
//        //   OR extract(YEAR from time) = someValue
//        //
//        // we cannot rewrite extract on MONTH.
//        if (timeUnit == TimeUnitRange.YEAR) {
//            return true;
//        }
//        final RangeSet<Calendar> calendarRangeSet = operandRanges.get(operand);
//        if (calendarRangeSet == null || calendarRangeSet.isEmpty()) {
//            return false;
//        }
//        for (Range<Calendar> range : calendarRangeSet.asRanges()) {
//            // Cannot reWrite if range does not have an upper or lower bound
//            if (!range.hasUpperBound() || !range.hasLowerBound()) {
//                return false;
//            }
//        }
        return true;
    }

    @Override
    protected List<RexNode> visitList(List<? extends RexNode> exprs,
                                      boolean[] update) {
//        if (exprs.isEmpty()) {
//            return ImmutableList.of(); // a bit more efficient
//        }
//        switch (calls.peek().getKind()) {
//            case AND:
//                return super.visitList(exprs, update);
//            default:
//                if (timeUnit != TimeUnitRange.YEAR) {
//                    // Already visited for lower TimeUnit ranges in the loop below.
//                    // Early bail out.
//                    //noinspection unchecked
//                    return (List<RexNode>) exprs;
//                }
//                final Map<RexNode, RangeSet<Calendar>> save =
//                        ImmutableMap.copyOf(operandRanges);
        final ImmutableList.Builder<RexNode> clonedOperands =
                ImmutableList.builder();
//                for (RexNode operand : exprs) {
//                    RexNode clonedOperand = operand;
//                    for (TimeUnitRange timeUnit : timeUnitRanges) {
//                        clonedOperand = clonedOperand.accept(
//                                new DateRangeRules.ExtractShuttle(rexBuilder, timeUnit, operandRanges,
//                                        timeUnitRanges, timeZone));
//                    }
//                    if ((clonedOperand != operand) && (update != null)) {
//                        update[0] = true;
//                    }
//                    clonedOperands.add(clonedOperand);
//
//                    // Restore the state. For an operator such as "OR", an argument
//                    // cannot inherit the previous argument's state.
//                    operandRanges.clear();
//                    operandRanges.putAll(save);
//                }
        return clonedOperands.build();
    }
}