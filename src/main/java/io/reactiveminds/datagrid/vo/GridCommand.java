package io.reactiveminds.datagrid.vo;

import java.io.Serializable;

public class GridCommand implements Serializable {
	//feeling lazy to make it DataSerializable
	public GridCommand(Command command, String args) {
		super();
		this.command = command;
		this.args = args;
	}
	public GridCommand() {
	}
	public static enum Command{FLUSH}
	/**
	 * 
	 */
	private static final long serialVersionUID = -8572160719816625902L;

	public Command getCommand() {
		return command;
	}
	public void setCommand(Command command) {
		this.command = command;
	}
	public String getArgs() {
		return args;
	}
	public void setArgs(String args) {
		this.args = args;
	}
	Command command;
	String args;
}
