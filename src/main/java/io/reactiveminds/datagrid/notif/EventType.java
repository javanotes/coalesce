package io.reactiveminds.datagrid.notif;

import org.slf4j.event.Level;

public enum EventType {

	MESSAGE_CREATE {
		@Override
		public String level() {
			return Level.INFO.name();
		}
	},
	MESSAGE_RECEIPT {
		@Override
		public String level() {
			return Level.DEBUG.name();		}
	},
	MESSAGE_REPEAT {
		@Override
		public String level() {
			return Level.WARN.name();
		}
	},
	APPLIED_TO_GRID {
		@Override
		public String level() {
			return Level.INFO.name();
		}
	},
	FLUSHED_TO_STORE {
		@Override
		public String level() {
			return Level.INFO.name();
		}
	};
	
	public abstract String level();
}
