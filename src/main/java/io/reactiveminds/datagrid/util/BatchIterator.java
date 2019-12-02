package io.reactiveminds.datagrid.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BatchIterator<E> implements Iterator<List<E>> {

	public BatchIterator(List<E> items, int batchSize) {
		super();
		this.items = items;
		this.batchSize = batchSize;
		iters = this.items.size() / this.batchSize;
	}

	private int i=1;
	private int j = 0;
	
	private int iters;
	
	private final List<E> items;
	private final int batchSize;
	
	
	@Override
	public boolean hasNext() {
		return j < items.size();
	}

	@Override
	public List<E> next() {
		if(!hasNext())
			throw new NoSuchElementException("at: "+j+", size: "+items.size());
		
		List<E> batch = new ArrayList<>(batchSize);
		if (i <= iters) {
			for (; j < i * batchSize; j++) {
				batch.add(items.get(j));
			}
			i++;
		}
		else {
			for (; j < items.size(); j++) {
				batch.add(items.get(j));
			}
		}
		return batch;
	}

}
