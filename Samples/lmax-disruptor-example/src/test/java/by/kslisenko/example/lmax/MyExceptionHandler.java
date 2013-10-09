package by.kslisenko.example.lmax;

import com.lmax.disruptor.ExceptionHandler;

public class MyExceptionHandler implements ExceptionHandler {

	public void handleEventException(Throwable ex, long sequence, Object event) {
		// TODO Auto-generated method stub

	}

	public void handleOnStartException(Throwable ex) {
		// TODO Auto-generated method stub

	}

	public void handleOnShutdownException(Throwable ex) {
		// TODO Auto-generated method stub

	}
}
