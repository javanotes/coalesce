package io.reactiveminds.datagrid.notif;

import org.slf4j.event.Level;

public enum EventType {

	CREATE {
		@Override
		public String level() {
			return Level.DEBUG.name();
		}
	},
	RECEIPT {
		@Override
		public String level() {
			return Level.INFO.name();		}
	},
	REPEAT {
		@Override
		public String level() {
			return Level.WARN.name();
		}
	},
	COALESCE {
		@Override
		public String level() {
			return Level.INFO.name();
		}
	},
	SAVED {
		@Override
		public String level() {
			return Level.INFO.name();
		}
	},
	DELETED {
		@Override
		public String level() {
			return Level.WARN.name();
		}
	};
	
	public abstract String level();
}
