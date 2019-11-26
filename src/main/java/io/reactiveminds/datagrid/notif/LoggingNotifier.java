package io.reactiveminds.datagrid.notif;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingNotifier extends AbstractEventsNotifier {

	private static final Logger log = LoggerFactory.getLogger("LoggingNotifier");
	@Override
	public void onApplicationEvent(EventNotification event) {
		log.info(event.toString());
	}

}
