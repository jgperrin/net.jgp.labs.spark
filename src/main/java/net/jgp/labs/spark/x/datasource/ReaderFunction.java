package net.jgp.labs.spark.x.datasource;

import java.io.Serializable;

import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.PartitionedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Function1;
import scala.collection.Iterator;

public final class ReaderFunction implements Serializable, Function1<PartitionedFile, Iterator<InternalRow>> {
	private static final long serialVersionUID = 7005714888056426503L;
	private static Logger log = LoggerFactory.getLogger(ReaderFunction.class);

	@Override
	public <A> Function1<PartitionedFile, A> andThen(Function1<Iterator<InternalRow>, A> arg0) {
		return null;
	}

	@Override
	public Iterator<InternalRow> apply(PartitionedFile arg0) {
		log.debug("-> apply({})", arg0.filePath());
		RDD<T> a;
		return null;
	}

	@Override
	public double apply$mcDD$sp(double arg0) {
		return 0;
	}

	@Override
	public double apply$mcDF$sp(float arg0) {
		return 0;
	}

	@Override
	public double apply$mcDI$sp(int arg0) {
		return 0;
	}

	@Override
	public double apply$mcDJ$sp(long arg0) {
		return 0;
	}

	@Override
	public float apply$mcFD$sp(double arg0) {
		return 0;
	}

	@Override
	public float apply$mcFF$sp(float arg0) {
		return 0;
	}

	@Override
	public float apply$mcFI$sp(int arg0) {
		return 0;
	}

	@Override
	public float apply$mcFJ$sp(long arg0) {
		return 0;
	}

	@Override
	public int apply$mcID$sp(double arg0) {
		return 0;
	}

	@Override
	public int apply$mcIF$sp(float arg0) {
		return 0;
	}

	@Override
	public int apply$mcII$sp(int arg0) {
		return 0;
	}

	@Override
	public int apply$mcIJ$sp(long arg0) {
		return 0;
	}

	@Override
	public long apply$mcJD$sp(double arg0) {
		return 0;
	}

	@Override
	public long apply$mcJF$sp(float arg0) {
		return 0;
	}

	@Override
	public long apply$mcJI$sp(int arg0) {
		return 0;
	}

	@Override
	public long apply$mcJJ$sp(long arg0) {
		// No clue what this is doing, inherited from interface.
		return 0;
	}

	@Override
	public void apply$mcVD$sp(double arg0) {
		// No clue what this is doing, inherited from interface.
	}

	@Override
	public void apply$mcVF$sp(float arg0) {
		// No clue what this is doing, inherited from interface.
	}

	@Override
	public void apply$mcVI$sp(int arg0) {
		// No clue what this is doing, inherited from interface.
	}

	@Override
	public void apply$mcVJ$sp(long arg0) {
		// No clue what this is doing, inherited from interface.
	}

	@Override
	public boolean apply$mcZD$sp(double arg0) {
		return false;
	}

	@Override
	public boolean apply$mcZF$sp(float arg0) {
		return false;
	}

	@Override
	public boolean apply$mcZI$sp(int arg0) {
		return false;
	}

	@Override
	public boolean apply$mcZJ$sp(long arg0) {
		return false;
	}

	@Override
	public <A> Function1<A, Iterator<InternalRow>> compose(Function1<A, PartitionedFile> arg0) {
		return null;
	}
}