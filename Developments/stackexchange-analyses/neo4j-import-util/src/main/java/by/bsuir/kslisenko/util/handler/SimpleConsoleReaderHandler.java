package by.bsuir.kslisenko.util.handler;

import org.apache.hadoop.io.Writable;


public class SimpleConsoleReaderHandler<K extends Writable, V extends Writable> extends ConsoleReaderHandler<K, V> {

	public SimpleConsoleReaderHandler() {
		super(new SimpleReaderHandler<K, V>());
	}
}
