package com.hortonworks.hbase.access;

import static java.lang.Math.abs;
import static java.lang.Math.pow;
import static java.lang.Math.sqrt;

import java.io.FileWriter;
import java.util.Random;

import org.apache.log4j.Logger;

import com.hortonworks.pojo.TAG;

public class DouglasPeuckerFilter  {

	private static final Logger logger = Logger.getLogger(DouglasPeuckerFilter.class);

	private double epsilon = 1.0;

	private static final String RAW_POINTS_FILE = "rawPoints";

	private static final String FILTERED_POINTS_FILE = "filteredPoints";


	public DouglasPeuckerFilter(double epsilon)
	{
        if (epsilon >= 0)

        	this.epsilon = epsilon;

        else

        	logger.info("[" + TimeStamp.getTimeStamp() + "] " +
        			"DouglasPeuckerFilter:: Epsilon should be greater than 0, defaulting to 1");
	}

	private double[] DPFilter(double[] points, int startIndex, int endIndex) throws Exception
	{
		try {

	       double maxDistance = 0;

	        int index = 0;

	        double a = endIndex - startIndex;

	        double b = points[endIndex] - points[startIndex];

	        double c = -(b * startIndex - a * points[startIndex]);

	        double norm = sqrt(pow(a, 2) + pow(b, 2));

	        for (int i = startIndex + 1; i < endIndex; i++)
	        {
	            double curPointDistance = abs(b * i - a * points[i] + c) / norm;

	            if (curPointDistance > maxDistance) {

	            	index = i;

	                maxDistance = curPointDistance;
	            }
		    }

	        if (maxDistance >= epsilon) {

	                double[] leftFilterResult = DPFilter(points, startIndex, index);

	                double[] rightFilterResult = DPFilter(points, index, endIndex);

	                double[] result = new double[(leftFilterResult.length - 1) + rightFilterResult.length];

	                System.arraycopy(leftFilterResult, 0, result, 0, leftFilterResult.length - 1);

	                System.arraycopy(rightFilterResult, 0, result, leftFilterResult.length - 1, rightFilterResult.length);

	                return result;

	        } else {

	                return new double[] { points[startIndex], points[endIndex] };
	        }
		}
		catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "DouglasPeuckerFilter::DPFilter::" +  e.getMessage(), e);

			throw e;
		}
	}


  private void dumpPoints(double[] points, String fileName)
  {
	  try {

			int noOfPointsPerLine = 32 ;

			FileWriter writer = new FileWriter(fileName, false);

	        for (int i = 0; i < points.length; i++)
	        {
	        	writer.append(Double.toString(points[i]));

	        	writer.append(" ");

	        	if ((i%noOfPointsPerLine) == 0)
	        	{
	        		writer.append("\n");

	        		writer.flush();
	        	}
	        }

        	writer.flush();

        	writer.close();
	  	}
		catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "DouglasPeuckerFilter::dumpPoints::" +  e.getMessage(), e);
		}
  }

	public TAG[] DPFilterTags(TAG[] tags, int startIndex, int endIndex) throws Exception
	{
		try {

			double maxDistance = 0.0, curPointDistance = 0.0;

	        int index = 0;

	        double a = endIndex - startIndex;

	        double b = tags[endIndex].value - tags[startIndex].value;

	        double c = -(b * startIndex - a * tags[startIndex].value);

	        double norm = sqrt(pow(a, 2) + pow(b, 2));

	        for (int i = startIndex + 1; i < endIndex; i++)
	        {
	            curPointDistance = abs(b * i - a * tags[i].value + c) / norm;

	            if (curPointDistance > maxDistance) {

	            	index = i;

	                maxDistance = curPointDistance;
	            }
		    }

	        logger.debug("[" + TimeStamp.getTimeStamp() + "] " + "DPFilterTags:: curDistance + maxDistance + epsilon :: "
	        			+  curPointDistance + " " + maxDistance + " " + epsilon);

	        if (maxDistance >= epsilon) {

		        	logger.debug("[" + TimeStamp.getTimeStamp() + "] " + "DPFilterTags:: LeftRecursion + startIndex + endIndex :: "
	        			+  startIndex + " " + index);

	    	        logger.debug("[" + TimeStamp.getTimeStamp() + "] " + "DPFilterTags:: RightRecursion + startIndex + endIndex :: "
		        			+  index + " " + endIndex);

	                TAG[] leftFilterResult = DPFilterTags(tags, startIndex, index);

	                TAG[] rightFilterResult = DPFilterTags(tags, index, endIndex);

	                TAG[] result = new TAG[(leftFilterResult.length - 1) + rightFilterResult.length];

	                System.arraycopy(leftFilterResult, 0, result, 0, leftFilterResult.length - 1);

	                System.arraycopy(rightFilterResult, 0, result, leftFilterResult.length - 1, rightFilterResult.length);

	                return result;

	        } else {

    	        	logger.debug("[" + TimeStamp.getTimeStamp() + "] " + "DPFilterTags:: NoRecursion + startIndex + endIndex :: "
	        			+  startIndex + " " + endIndex);

	                return new TAG[] { tags[startIndex], tags[endIndex] };
	        }
		}
		catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "DouglasPeuckerFilter::DPFilterTags::" +  e.getMessage(), e);

			throw e;
		}
	}

  public static void main(String args[])
  {
	  try {

	        Random random = new Random();

	        int noOfPoints, epsilon, randomInterval;

	        if (args.length < 3)
	        {
	        	System.out.println("Usage <randomInterval> <epsilon> <noOfRawPoints> ");

	        	System.exit(0);
	        }

	        randomInterval = new Integer(args[0]);

	        epsilon = new Integer(args[1]);

	        noOfPoints = new Integer(args[2]);

			boolean dumpResults = false;

			if (args.length > 3) {

				if (new Integer(args[3]).intValue() ==  1)
					dumpResults = true;
				else
					dumpResults = false;
			}

	    	logger.info("[" + TimeStamp.getTimeStamp() + "] " +
	    			"DouglasPeuckerFilter:: RandomInterval + Epsilon + NoOfRawPoints + DumpResults ::" +
	    			randomInterval + " " + epsilon + " " + noOfPoints + " " + dumpResults);

	        double[] rawPoints = new double[noOfPoints];

	        for (int i = 0; i < rawPoints.length; i++)

	        	rawPoints[i] = random.nextInt(randomInterval);

	        DouglasPeuckerFilter dpFilter = new DouglasPeuckerFilter(epsilon);

	        double[] filteredPoints = dpFilter.DPFilter(rawPoints, 0, rawPoints.length - 1);

	        System.out.println("Filtering Complete.. Writing Results...");

	        if (dumpResults) {
	        	dpFilter.dumpPoints(rawPoints, RAW_POINTS_FILE);

	        	dpFilter.dumpPoints(filteredPoints, FILTERED_POINTS_FILE);
	        }

	  }
	  catch (Exception e) {

			e.printStackTrace(System.err);

			logger.error("[" + TimeStamp.getTimeStamp() + "] " + "DouglasPeuckerFilter::main::" +  e.getMessage(), e);
		}
  }
}