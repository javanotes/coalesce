package io.reactiveminds.datagrid.notif;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingNotifier extends AbstractEventsNotifier {

	private static final Logger log = LoggerFactory.getLogger("LoggingNotifier");
	@Override
	public void onApplicationEvent(EventNotification event) {
		switch(event.event.level()) {
		case "DEBUG":
			log.debug(event.toString());
			break;
		case "WARN":
			log.warn(event.toString());
			break;
		case "ERROR":
			log.error(event.toString());
			break;
			default: log.info(event.toString()); break;
		}
		
	}

}
