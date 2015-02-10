package com.hortonworks.pojo;

public class TAG  {

	public TAG(String tname, long tstmp, int id, double val)
	{
		//tagname = tname;

		ts = tstmp;

		Id = id;

		value = val;
	}

	public String tagname;

	public long ts;

	public int Id;

	public double value;
}
