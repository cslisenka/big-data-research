package by.bsuir.kslisenko.util;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.Writable;

public interface ReaderHandler<K extends Writable, V extends Writable> {
	
	public void before() throws IOException;
	
	public void read(K key, V value, PrintStream out) throws IOException;
	
	public void after() throws IOException;
}