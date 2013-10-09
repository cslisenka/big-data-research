package by.kslisenko.example.lmax;

import com.lmax.disruptor.EventFactory;

public class MyEventFactory implements EventFactory<MyStruct> {

	public MyStruct newInstance() {
		return new MyStruct();
	}
}
