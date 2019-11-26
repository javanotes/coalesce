package io.reactiveminds.datagrid.mapper;

public class FieldMapping{
	@Override
	public String toString() {
		return "FieldMapping [sourceField=" + sourceField + ", expr=" + expr + ", targetField=" + targetField + "]";
	}

	public FieldMapping() {
	}
	
	public FieldMapping(String sourceField, String targetField) {
		super();
		this.sourceField = sourceField;
		this.targetField = targetField;
	}

	/**
	 * source field path
	 * <i>'fieldOne.fieldOneChildOneArray[fieldOneChildOneDiscriminatorField, DiscriminatorValue]'</i>
	 * @return
	 */
	public String getSourceField() {
		return sourceField;
	}
	public void setSourceField(String sourceField) {
		this.sourceField = sourceField;
	}
	/**
	 * target field path <i>'fieldOne.fieldOneChildOne.fieldOneChildOneFieldOne'</i>
	 * @return
	 */
	public String getTargetField() {
		return targetField;
	}
	public void setTargetField(String targetField) {
		this.targetField = targetField;
	}
	public String getExpr() {
		return expr;
	}

	public void setExpr(String expr) {
		this.expr = expr;
	}
	
	public boolean isDerived() {
		return derived;
	}

	public void setDerived(boolean derived) {
		this.derived = derived;
	}

	private String sourceField;
	private String expr;
	private String targetField;
	private boolean derived;
}