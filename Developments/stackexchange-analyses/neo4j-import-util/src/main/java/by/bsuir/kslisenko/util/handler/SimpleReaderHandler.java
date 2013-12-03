package by.bsuir.kslisenko.util.handler;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.Writable;

import by.bsuir.kslisenko.util.ReaderHandler;

public class SimpleReaderHandler<K extends Writable, V extends Writable> implements ReaderHandler<K, V> {
	
	@Override
	public void before() throws IOException {
	}
	
	@Override
	public void read(K key, V value, PrintStream out) throws IOException {
		out.println(key + "\t" + value);
	}
	
	@Override
	public void after() throws IOException {
	}	
}
