package io.reactiveminds.datagrid.notif;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import io.reactiveminds.datagrid.util.Utils;

public class KafkaNotifier extends AbstractEventsNotifier {
	
	@Autowired
	KafkaTemplate<String, String> kafkaTmpl;
	@Value("${coalesce.core.notification.topic:")
	private String topic;

	@Override
	public void onApplicationEvent(EventNotification event) {
		kafkaTmpl.send(topic, Utils.toPrettyJsonString(event));
	}
}
