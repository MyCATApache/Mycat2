package io.mycat.util;

import java.util.*;
import java.util.function.Function;

/**
 * 常用于对集合进行转换的场景, 比较好的可读性(相比于Lamaba表达式).
 * 效果类似于 list.stream().map().collect(Collectors.toList()), 不同点是它是懒执行的. 而且每个元素只会执行一次transform.apply().
 *
 * 使用方式 {@link LazyTransformCollection#transform(Iterable, Function)}
 *
 * 调试时比较方便
 * 1. 可以看到转换前和转换后的每个元素
 * 2. 可以看到经历多次转换的联动关系 (可以方便排查哪次转换出bug了)
 * 3. 可读的toString()方法.
 *
 * @param <I> 输入元素类型
 * @param <O> 输出元素类型
 */
public class LazyTransformCollection<I,O> extends AbstractCollection<O> implements Iterable<O>,Collection<O> {
    private final Iterable<I> inputList;
    private volatile Collection<O> outputList;
    private final Function<I,O> transform;

    public LazyTransformCollection(Iterable<I> inputList, Function<I,O> transform) {
        this.inputList = Objects.requireNonNull(inputList);
        this.transform = transform;
    }

    public static <I,O>Collection<O> transform(Iterable<I> list, Function<I,O> transform) {
        return new LazyTransformCollection<>(list,transform);
    }

    public Collection<O> getOutputList() {
        if(outputList == null){
            List<O> list = new ArrayList<>();
            for(I i : inputList){
                list.add(transform.apply(i));
            }
            outputList = list;
        }
        return outputList;
    }

    @Override
    public int size() {
        if(outputList == null && inputList instanceof Collection){
            return ((Collection<I>) inputList).size();
        }else {
            return getOutputList().size();
        }
    }

    @Override
    public boolean isEmpty() {
        if(outputList == null) {
            if(inputList instanceof Collection && !(inputList instanceof LazyTransformCollection)){
                return ((Collection<I>) inputList).isEmpty();
            }else {
                return !inputList.iterator().hasNext();
            }
        }else {
            return getOutputList().isEmpty();
        }
    }

    @Override
    public boolean contains(Object o) {
        return getOutputList().contains(o);
    }

    @Override
    public Iterator<O> iterator() {
        if(outputList == null) {
            return new IteratorImpl<>(inputList.iterator(), transform,this);
        }else {
            return getOutputList().iterator();
        }
    }

    @Override
    public Object[] toArray() {
        Object[] array = new Object[size()];
        int i=0;
        for(O o : this){
            array[i++] = o;
        }
        return array;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return getOutputList().toArray(a);
    }

    @Override
    public boolean add(O o) {
        return getOutputList().add(o);
    }

    @Override
    public boolean remove(Object o) {
        return getOutputList().remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return getOutputList().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends O> c) {
        return getOutputList().addAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return getOutputList().removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return getOutputList().retainAll(c);
    }

    @Override
    public void clear() {
        getOutputList().clear();
    }

    static class IteratorImpl<I,O> implements Iterator<O>{
        private final Iterator<I> iterator;
	    private final Function<I,O> transform;
        private final LazyTransformCollection parent;
        private final List<O> cacheList;
	    IteratorImpl(Iterator<I> iterator, Function<I, O> transform, LazyTransformCollection parent) {
		    this.iterator = iterator;
		    this.transform = transform;
		    this.parent = parent;
		    if(parent.inputList instanceof Collection && !(parent.inputList instanceof LazyTransformCollection)){
		        cacheList = new ArrayList<>(((Collection) parent.inputList).size());
            }else {
		        cacheList = new ArrayList<>();
            }
	    }

	    @Override
        public boolean hasNext() {
            boolean next = iterator.hasNext();
            if(!next){
                parent.outputList = cacheList;
            }
            return next;
        }

        @Override
        public O next() {
            O next = transform.apply(iterator.next());
            cacheList.add(next);
            return next;
        }
    }

}
