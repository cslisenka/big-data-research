package by.bsuir.kslisenko.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;

import by.bsuir.kslisenko.util.handler.SimpleConsoleReaderHandler;

/**
 * Utility for reading mahout sequence files 
 * @author kslisenko
 */
public class SequenceFileReaderUtil {
	
	public static <K extends Writable, V extends Writable> void readPartFileToConsole(String path, int rows, Configuration conf) throws IOException {
		readPartFile(path, rows, conf, new SimpleConsoleReaderHandler<K, V>());
	}
	
	public static <K extends Writable, V extends Writable> void readPartFile(String path, int rows, Configuration conf, ReaderHandler<K, V> handler) throws IOException {
		System.out.println("==============================================================");
		System.out.println("Reading file: " + path + "\n");
		iterate(new SequenceFileIterable<K, V>(new Path(path), conf), rows, handler);
	}

	public static <K extends Writable, V extends Writable> void readPartFilesInDirToConsole(String path, int rows, Configuration conf) throws IOException {
		readPartFilesInDir(path, rows, conf, new SimpleConsoleReaderHandler<K, V>());
	}
	
	public static <K extends Writable, V extends Writable> void readPartFilesInDir(String path, int rows, Configuration conf, ReaderHandler<K, V> handler) throws IOException {
		System.out.println("==============================================================");
		System.out.println("Reading files in: " + path + "\n");
		iterate(new SequenceFileDirIterable<K, V>(new Path(path + "/*"), PathType.GLOB, PathFilters.partFilter(), conf), rows, handler);
	}	
	
	private static <K extends Writable, V extends Writable> void iterate(Iterable<Pair<K, V>> iterable, int rows, ReaderHandler<K, V> handler) throws IOException {
		handler.before();
		int counter = 0;
		for (Pair<K, V> pair : iterable) {
			if (counter++ >= rows) {
				break;
			}
			handler.read(pair.getFirst(), pair.getSecond(), System.out);
		}
		handler.after();
	}
}