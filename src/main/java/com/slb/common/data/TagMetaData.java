package com.slb.common.data;

public class TagMetaData {

	private String tag;
	private String pump;
	private int tagId;
	private int pumpId;
	private String tagName;
	private String tagType;
	private String tagUnit;
	private Double lowValue;
	private Double lowLowValue;
	private Double highValue;
	private Double highHighValue;
	private String uom;
	private String desc;

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public String getPump() {
		return pump;
	}

	public void setPump(String pump) {
		this.pump = pump;
	}

	public int getTagId() {
		return tagId;
	}
	public void setTagId(int tagId) {
		this.tagId = tagId;
	}
	public int getPumpId() {
		return pumpId;
	}
	public void setPumpId(int pumpId) {
		this.pumpId = pumpId;
	}
	public String getTagType() {
		return tagType;
	}
	public void setTagType(String tagType) {
		this.tagType = tagType;
	}
	public String getTagUnit() {
		return tagUnit;
	}
	public void setTagUnit(String tagUnit) {
		this.tagUnit = tagUnit;
	}
	public Double getLowValue() {
		return lowValue;
	}
	public void setLowValue(Double lowValue) {
		this.lowValue = lowValue;
	}
	public Double getLowLowValue() {
		return lowLowValue;
	}
	public void setLowLowValue(Double lowLowValue) {
		this.lowLowValue = lowLowValue;
	}
	public Double getHighValue() {
		return highValue;
	}
	public void setHighValue(Double highValue) {
		this.highValue = highValue;
	}
	public Double getHighHighValue() {
		return highHighValue;
	}
	public void setHighHighValue(Double highHighValue) {
		this.highHighValue = highHighValue;
	}
	public String getUom() {
		return uom;
	}
	public void setUom(String uom) {
		this.uom = uom;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}

	public String getTagName() {
		return tagName;
	}

	public void setTagName(String tagName) {
		this.tagName = tagName;
	}

	public String toString(){
		return "tagId=" + tagId + "|tag=" + tag + "|tagType=" + tagType + "|highValue=" + highValue + "|highHighValue="+ highHighValue + "|lowValue=" + lowValue + "|lowLowValue=" + lowLowValue+ "|uom=" + uom;
	}
}
