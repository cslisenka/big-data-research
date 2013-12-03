package by.bsuir.kslisenko.util.handler;

import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.io.Writable;

import by.bsuir.kslisenko.util.ReaderHandler;

public class ConsoleReaderHandler<K extends Writable, V extends Writable> implements ReaderHandler<K, V> {

	private ReaderHandler<K, V> handler;
	
	public ConsoleReaderHandler(ReaderHandler<K, V> handler) {
		this.handler = handler;
	}
	
	@Override
	public void before() throws IOException {
	}
	
	@Override
	public void read(K key, V value, PrintStream out) throws IOException {
		handler.read(key, value, System.out);
	}
	
	@Override
	public void after() throws IOException {
	}	
}
