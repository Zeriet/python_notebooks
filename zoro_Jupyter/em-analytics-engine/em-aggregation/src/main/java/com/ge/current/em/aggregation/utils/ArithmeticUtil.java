package com.ge.current.em.aggregation.utils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class ArithmeticUtil {

	private static Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>> 
        maxReduceFunc =
			new Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>>() {
			
		@Override
		public Map<String, Double> call(Map<String, Double> m1, Map<String, Double> m2) {
			if(m2 == null) return m1;
			if(m1 == null) return m2;
			if(m1 == null && m2 == null) return new HashMap<>();
			Map<String, Double> maxMap = new HashMap<String, Double>();
			
			for(String key: m1.keySet()) {
					maxMap.put(key,  m1.get(key));
				}
			
			for(String key: m2.keySet()) {
				if (maxMap.containsKey(key)) {
					maxMap.put(key,  Math.max(m1.get(key), m2.get(key)));
				} else {
					maxMap.put(key, m2.get(key));
				}
			}
			return maxMap;
		}
	};
	
	private static Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>> 
        minReduceFunc =
			new Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>>() {
		
		@Override
		public Map<String, Double> call(Map<String, Double> m1, Map<String, Double> m2) {
			if(m2 == null) return m1;
			if(m1 == null) return m2;
			if(m1 == null && m2 == null) return new HashMap<>();
			HashMap<String, Double> minMap = new HashMap<String, Double>();				
			for(String key: m1.keySet()) {
				minMap.put(key, m1.get(key));
			}
			
			for(String key: m2.keySet()) {
				if (minMap.containsKey(key)) {
					minMap.put(key, Math.min(m1.get(key), m2.get(key)));
				} else {
					minMap.put(key, m2.get(key));
				}
			}
			return minMap;
		};
	};
	
	private static Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>> 
        sumReduceFunc = 
			new Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>>() {
        
    	@Override 
    	public Map<String, Double> call(Map<String, Double> m1, Map<String, Double> m2) {
    		if(m2 == null) return m1;
			if(m1 == null) return m2;
			if(m1 == null && m2 == null) return new HashMap<>();
    		Map<String, Double> sumMap = new HashMap<String, Double>();
    		
    		for(String key: m1.keySet()) {
    			sumMap.put(key, m1.get(key));
    		}            
    		
    		for(String key: m2.keySet()) {
    			if (m1.containsKey(key) && m2.containsKey(key)) {
    				sumMap.put(key, m1.get(key) + m2.get(key));
    			} else {
    				sumMap.put(key, m2.get(key));
    			}
            }      
            return sumMap;
        };
	};
	
	//this function is to calculate the count of each key occurrence. 
	private static Function<Map<String, Double>, Map<String, Double>> cntMapperFunc = 
		new Function<Map<String, Double>, Map<String, Double>>(){		
		@Override
		public Map<String, Double> call(Map<String, Double> orig) {
			
			Map<String, Double> newMap = new HashMap<>();
			
			for(String key: orig.keySet()) {
				newMap.put(key, 1.0);
			}
			return newMap;
		}};
		

		
	public static class AvgCount implements Serializable {
		public double total_;
		public int num_;
		public AvgCount(double total, int num) {
			total_ = total;
			num_ = num; 
		}
		public void setTotal(double total) {
			total_ = total;
		}
		public void setNum(int num) {
			num_ = num;
		}
		public double avg() {
			return total_ / num_; 
		}
	};
	
	private static Function<Map<String, Double>, Map<String, AvgCount>> createAccFunc = 
			new Function<Map<String, Double>, Map<String, AvgCount>>() {
		
				public Map<String, AvgCount> call(Map<String, Double> map) {
					Map<String, AvgCount> cntMap = new HashMap<>();
					for(String key: map.keySet()) {
						AvgCount avgCnt = new AvgCount(map.get(key), 1);
						cntMap.put(key, avgCnt);
					}
					return cntMap;
				}
	};

	// update the AvgCount 
	private static Function2<Map<String, AvgCount>, Map<String, Double>, Map<String, AvgCount>> addAndCountFunc = 
			new Function2<Map<String, AvgCount>, Map<String, Double>, Map<String, AvgCount>>() {
		
		public Map<String, AvgCount> call(Map<String, AvgCount> m1, Map<String, Double> m2) {
			for(String key: m2.keySet()) {
				if (m1.containsKey(key)) {
					AvgCount avgCnt = m1.get(key);
					avgCnt.setTotal(avgCnt.total_ + m2.get(key));
					avgCnt.setNum(avgCnt.num_+1);	
				} else {
					AvgCount avgCnt = new AvgCount(m2.get(key), 1);
					m1.put(key, avgCnt);
				}
			}
			return m1;
		}
	};
	
	// combine two avgCount results and return. 
	private static Function2<Map<String, AvgCount>, Map<String, AvgCount>, Map<String, AvgCount>> combineFunc = 
			new Function2<Map<String, AvgCount>, Map<String, AvgCount>, Map<String, AvgCount>>() {
		public Map<String, AvgCount> call(Map<String, AvgCount> m1, Map<String, AvgCount> m2) {
			for(String key: m2.keySet()) {
				if (m1.containsKey(key)) {
					AvgCount avgCnt = m1.get(key);
					avgCnt.setTotal(avgCnt.total_ + m2.get(key).total_);
					avgCnt.setNum(avgCnt.num_+m2.get(key).num_);
				} else {
					m1.put(key,  m2.get(key));
				}
			}
			return m1;
		}
	};
	
	//this function is to calculate the count of each key occurrence in Tag. 
	private static Function<Map<String, String>, Map<String, Double>> tagCntMapperFunc = 
			new Function<Map<String, String>, Map<String, Double>>(){		
		@Override
		public Map<String, Double> call(Map<String, String> origTag) {
			Map<String, Double> newMap = new HashMap<>();
			for(String key: origTag.keySet()) {
				String combinedKey = key+ ":"+ origTag.get(key);
				newMap.put(combinedKey, 1.0);
			}
			return newMap;
		}};		
		
	
	public static Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>> getMaxReduceFunc() {
		return maxReduceFunc;
	}
	
	public static Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>> getMinReduceFunc() {
		return minReduceFunc;
	}
	
	public static Function2<Map<String, Double>, Map<String, Double>, Map<String, Double>> getSumReduceFunc() {
		return sumReduceFunc;
	}
	
	public static Function<Map<String, Double>, Map<String, Double>> getMeasuresCntMapperFunc() {
		return cntMapperFunc;
	}
	
	public static Function<Map<String, Double>, Map<String, AvgCount>> getCreateAccFunc() {
		return createAccFunc;
	}
	
	public static Function2<Map<String, AvgCount>, Map<String, Double>, Map<String, AvgCount>> getAddAndCountFunc() {
		return addAndCountFunc;
	}
	public static Function2<Map<String, AvgCount>, Map<String, AvgCount>, Map<String, AvgCount>> getCombineFunc() {
		return combineFunc;
	}		

	public static Function<Map<String, String>, Map<String, Double>> getTagCntMapperFunc() {
		return tagCntMapperFunc;
	}
}
