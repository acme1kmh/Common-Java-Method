package aws.cloud.storage;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.lguplus.vr.admin.category.model.CategoryZtreeVO;
import com.lguplus.vr.admin.util.UUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class AwsDynamoDB extends AwsConfig{

	private String dynamoDBTableName;
	private AmazonDynamoDB ddb;


	/**
	 * @throws Exception
	 */
	public AwsDynamoDB() throws Exception {
		super();
		dynamoDBTableName = AwsConfig.getDynamoDBTableName();
		ddb = AmazonDynamoDBClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds))
				.withRegion(getRegions())
				.build();
	}

	/**
	 * 일 UV
	 * @param date
	 * @return
	 * @throws Exception
	 */
	@Deprecated
	public int dailyUserDynamoScan(String date) throws Exception{
		if(AwsConfig.isAwsMode() == false) throw new Exception();

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		int getCount = 0;
		
		ScanRequest request = new ScanRequest();

		expressionAttributeValues.put(":date", new AttributeValue().withS(date));

		do {
			request = new ScanRequest()
					.withTableName(dynamoDBTableName)
					.withFilterExpression("meta = :date")
					.withExpressionAttributeValues(expressionAttributeValues)
					.withProjectionExpression("sid")
					.withExclusiveStartKey(lastKeyEvaluated);
	
			ScanResult response = ddb.scan(request);
			getCount += response.getCount();
			lastKeyEvaluated = response.getLastEvaluatedKey();
		} while (null != lastKeyEvaluated);
		
		return getCount;
	}

	/**
	 * 일 UV
	 * @param date
	 * @return
	 * @throws Exception
	 */
	public List<Map<String,Object>> periodicUserDynamoScan(
			String startDt
			, String endDt
			) throws Exception{
		if(AwsConfig.isAwsMode() == false) throw new Exception();

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		List<Map<String, AttributeValue>> putItem = new ArrayList<>(); 
		ScanRequest request = new ScanRequest();

		expressionAttributeValues.put(":startDate", new AttributeValue().withS(startDt));
		expressionAttributeValues.put(":endDate", new AttributeValue().withS(endDt));
		do {
			request = new ScanRequest()
					.withTableName(dynamoDBTableName)
					.withFilterExpression("meta >= :startDate and meta <= :endDate")
					.withExpressionAttributeValues(expressionAttributeValues)
					.withProjectionExpression("sid, meta, totalContCnt, totalContCntDaily")
					.withExclusiveStartKey(lastKeyEvaluated);
			
			ScanResult response = ddb.scan(request);
			for (Map<String, AttributeValue> item : response.getItems()){
		        putItem.add(item);
		    }
			lastKeyEvaluated = response.getLastEvaluatedKey();
		} while (null != lastKeyEvaluated);

		String metaKey = "";
		Map<String, Object> uvMap = new HashMap<String, Object>();
		Map<String, Object> contUvMap = new HashMap<String, Object>();
		Map<String, Object> contPvMap = new HashMap<String, Object>();
		Map<String, Object> resultMap = new HashMap<String, Object>();

		for(Map<String, AttributeValue> item : putItem) {
			metaKey = item.get("meta").getS();
			int uv = 0;
			int contUV = 0;
			int contPV = 0;
			int totalContCnt = Integer.parseInt(item.get("totalContCntDaily").getN());

			//일UV
			if(uvMap.get(metaKey) != null) {
				uv =  (int) uvMap.get(metaKey);
			}
			uvMap.put(metaKey, ++uv);

			//콘탠츠UV
			if(totalContCnt > 0) {
				if(contUvMap.get(metaKey) != null) {
					contUV =  (int) contUvMap.get(metaKey);
				}
				contUvMap.put(metaKey, ++contUV);
			}

			//콘텐츠PV
			if(contPvMap.get(metaKey) != null) {
				contPV =  (int) contPvMap.get(metaKey);
			}
			contPvMap.put(metaKey, contPV + totalContCnt);

		}

		resultMap.put("uvMap", uvMap);
		resultMap.put("contUvMap", contUvMap);
		resultMap.put("contPvMap", contPvMap);

		log.debug("map result ::: " + resultMap);

		List<Map<String,Object>> resultList = new ArrayList<Map<String,Object>>();
		DateFormat df = new SimpleDateFormat("yyyyMMdd");
		Calendar startCal = Calendar.getInstance();
		Calendar endCal = Calendar.getInstance();

		startCal.set(Calendar.YEAR, Integer.parseInt(startDt.substring(0, 4)));
		startCal.set(Calendar.MONTH, Integer.parseInt(startDt.substring(4, 6)) - 1);
		startCal.set(Calendar.DATE, Integer.parseInt(startDt.substring(6, 8)));
		endCal.set(Calendar.YEAR, Integer.parseInt(endDt.substring(0, 4)));
		endCal.set(Calendar.MONTH, Integer.parseInt(endDt.substring(4, 6)) - 1);
		endCal.set(Calendar.DATE, Integer.parseInt(endDt.substring(6, 8)));
		while(startCal.compareTo(endCal) != 1) {
			String date = String.valueOf(df.format(startCal.getTime()));
			Map<String, Object> map = new HashMap<String, Object>();

			int uv = UUtil.nvl2(String.valueOf(uvMap.get(date)));
			int contPV = UUtil.nvl2(String.valueOf(contUvMap.get(date)));
			int contUV = UUtil.nvl2(String.valueOf(contPvMap.get(date)));

			map.put("date", date);
			map.put("uv", uv);
			map.put("contUV", contUV);
			map.put("contPV", contPV);
			resultList.add(map);
			startCal.add(Calendar.DATE, 1);

		}

		return resultList;
	}



	/**
	 * 일 콘텐츠이용UV
	 * @param date
	 * @return
	 * @throws Exception
	 */
	@Deprecated
	public int dailyContUserDynamoScan(String date) throws Exception{
		if(AwsConfig.isAwsMode() == false) throw new Exception();

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		ScanRequest request = new ScanRequest();
		int getCount = 0;
		
		expressionAttributeValues.put(":meta", new AttributeValue().withS(date));
		expressionAttributeValues.put(":zero", new AttributeValue().withN("0"));

		do {
			request = new ScanRequest()
					.withTableName(dynamoDBTableName)
					.withFilterExpression("meta = :meta and totalContCntDaily > :zero")
					.withExpressionAttributeValues(expressionAttributeValues)
					.withProjectionExpression("sid")
					.withExclusiveStartKey(lastKeyEvaluated);
			
			ScanResult response = ddb.scan(request);
			getCount += response.getCount();
			lastKeyEvaluated = response.getLastEvaluatedKey();
		} while (null != lastKeyEvaluated);

		return getCount;
	}


	/**
	 * 일 콘텐츠 이용PV
	 * @param date
	 * @return
	 * @throws Exception
	 */
	@Deprecated
	public int dailyContExecDynamoScan(String date) throws Exception{
		if(AwsConfig.isAwsMode() == false) throw new Exception();

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		List<Map<String, AttributeValue>> putItem = new ArrayList<>(); 
		ScanRequest request = new ScanRequest();

		expressionAttributeValues.put(":meta", new AttributeValue().withS(date));
		expressionAttributeValues.put(":zero", new AttributeValue().withN("0"));
		
		do {
			request = new ScanRequest()
					.withTableName(dynamoDBTableName)
					.withFilterExpression("meta = :meta and totalContCntDaily > :zero")
					.withExpressionAttributeValues(expressionAttributeValues)
					.withProjectionExpression("sid, totalContCntDaily")
					.withExclusiveStartKey(lastKeyEvaluated);
			
			ScanResult response = ddb.scan(request);
			for (Map<String, AttributeValue> item : response.getItems()){
		        putItem.add(item);
		    }
			lastKeyEvaluated = response.getLastEvaluatedKey();
		} while (null != lastKeyEvaluated);

		int resultCnt = 0;
		for (Map<String, AttributeValue> item : putItem) {
			resultCnt += Integer.parseInt(item.get("totalContCntDaily").getN());
		}
		return resultCnt;
	}


	/**
	 * 콘텐츠 편성 메뉴 통계 (시청자수/시청건수/시청시간)
	 * @param startDt
	 * @param endDt
	 * @param orgMenuCode
	 * @param beforeAt
	 * @return
	 * @throws Exception
	 */
	public Map<String,Object> contOrgMenuExec(
			String startDt
			, String endDt
			, List<CategoryZtreeVO> orgMenuCode
			, boolean beforeAt
			) throws Exception{
		if(AwsConfig.isAwsMode() == false) throw new Exception();

		Map<String, Object> resultMap = new HashMap<String, Object>();
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		List<Map<String, AttributeValue>> putItem = new ArrayList<>(); 
		int getCount = 0;
		
		ScanRequest request = new ScanRequest();

		expressionAttributeValues.put(":metaStartDt", new AttributeValue().withS("Execute-" + startDt));
        expressionAttributeValues.put(":metaEndDt", new AttributeValue().withS("Execute-" + UUtil.getTmrDateYYYYMMDD(endDt)));
        do {
        	request = new ScanRequest()
        			.withTableName(dynamoDBTableName)
        			.withFilterExpression("meta >= :metaStartDt and meta <= :metaEndDt")
        			.withExpressionAttributeValues(expressionAttributeValues)
        			.withProjectionExpression("sid, orgMenuCode, totalActionTime, totalContCnt")
        			.withExclusiveStartKey(lastKeyEvaluated);
        	
        	ScanResult response = ddb.scan(request);
        	for (Map<String, AttributeValue> item : response.getItems()){
		        putItem.add(item);
		    }
        	getCount += response.getCount();
        	lastKeyEvaluated = response.getLastEvaluatedKey();
		} while (null != lastKeyEvaluated);

		Map<String, Integer> execUser = new HashMap<String, Integer>();
		Map<String, Integer> execCont = new HashMap<String, Integer>();
		Map<String, Integer> execTime = new HashMap<String, Integer>();

		
		if(!putItem.isEmpty()) {
			for (Map<String, AttributeValue> item : putItem) {
				String orgMenuKey = item.get("orgMenuCode").getS();

				//편성메뉴 셋팅 (default 처리)
				for(int i=0; i<orgMenuCode.size(); i++) {
					String code = orgMenuCode.get(i).getOrgMenuCode();
					//2depth 까지만 담기
					if(!code.contains(orgMenuKey)) {
						if(code.length() <= 5) {
							execUser.put(code, UUtil.nvl2("null"));
							execCont.put(code, UUtil.nvl2("null"));
							execTime.put(code, UUtil.nvl2("null"));
						}
					}
				}

				//편성메뉴별 통계
				if(orgMenuKey.length() > 5) {
					orgMenuKey = orgMenuKey.substring(0, 6);
				}

				if(execCont.containsKey(orgMenuKey)) {
					execUser.put(orgMenuKey, execUser.get(orgMenuKey) + getCount);
					execCont.put(orgMenuKey, execCont.get(orgMenuKey) + Integer.parseInt(item.get("totalContCnt").getN()));
					execTime.put(orgMenuKey, execTime.get(orgMenuKey) + Integer.parseInt(item.get("totalActionTime").getN()));
				}else {
					execUser.put(orgMenuKey, getCount);
					execCont.put(orgMenuKey, Integer.parseInt(item.get("totalContCnt").getN()));
					execTime.put(orgMenuKey, Integer.parseInt(item.get("totalActionTime").getN()));
				}
			}

		}else {
			//편성메뉴 셋팅 (default 처리)
			for(int i=0; i<orgMenuCode.size(); i++) {
				String code = orgMenuCode.get(i).getOrgMenuCode();

				if(code.length() <= 5) {
					execUser.put(code, UUtil.nvl2("null"));
					execCont.put(code, UUtil.nvl2("null"));
					execTime.put(code, UUtil.nvl2("null"));
				}
			}
		}

		execTime.replaceAll((k, v) -> v / 60);

		resultMap.put("orgMenuCode", orgMenuCode);
		resultMap.put("startDt", startDt);
		resultMap.put("execUser", execUser);
		resultMap.put("execCont", execCont);
		resultMap.put("execTime", execTime);

		if(beforeAt) {
			Map<String, Integer> b_execUser = new HashMap<String, Integer>();
			Map<String, Integer> b_execCont = new HashMap<String, Integer>();
			Map<String, Integer> b_execTime = new HashMap<String, Integer>();

			getCount = 0;
			lastKeyEvaluated = null;
			putItem.clear();
			
			ScanRequest beforeRequest = new ScanRequest();

			endDt = startDt;
			startDt = UUtil.getBeforeDateYYYYMMDD(startDt);
			expressionAttributeValues.put(":metaStartDt", new AttributeValue().withS("Execute-" + startDt));
	        expressionAttributeValues.put(":metaEndDt", new AttributeValue().withS("Execute-" + endDt));
	        
	        do {
	        	beforeRequest = new ScanRequest()
	        			.withTableName(dynamoDBTableName)
	        			.withFilterExpression("meta >= :metaStartDt and meta <= :metaEndDt")
	        			.withExpressionAttributeValues(expressionAttributeValues)
	        			.withProjectionExpression("sid, orgMenuCode, totalActionTime, totalContCnt")
	        			.withExclusiveStartKey(lastKeyEvaluated);
	        	
	        	ScanResult beforeResponse = ddb.scan(beforeRequest);
	        	for (Map<String, AttributeValue> item : beforeResponse.getItems()){
			        putItem.add(item);
			    }
	        	getCount += beforeResponse.getCount();
	        	lastKeyEvaluated = beforeResponse.getLastEvaluatedKey();
			} while (null != lastKeyEvaluated);

			if(!putItem.isEmpty()) {
				for (Map<String, AttributeValue> item : putItem) {
					String orgMenuKey = item.get("orgMenuCode").getS();

					//편성메뉴 셋팅 (default 처리)
					for(int i=0; i<orgMenuCode.size(); i++) {
						String code = orgMenuCode.get(i).getOrgMenuCode();

						//2depth 까지만 담기
						if(!code.contains(orgMenuKey)) {
							if(code.length() <= 5) {
								b_execUser.put(code, UUtil.nvl2("null"));
								b_execCont.put(code, UUtil.nvl2("null"));
								b_execTime.put(code, UUtil.nvl2("null"));
							}
						}
					}

					//편성메뉴별 통계
					if(orgMenuKey.length() > 5) {
						orgMenuKey = orgMenuKey.substring(0, 6);
					}

					if(b_execCont.containsKey(orgMenuKey)) {
						b_execUser.put(orgMenuKey, b_execUser.get(orgMenuKey) + getCount);
						b_execCont.put(orgMenuKey, b_execCont.get(orgMenuKey) + Integer.parseInt(item.get("totalContCnt").getN()));
						b_execTime.put(orgMenuKey, b_execTime.get(orgMenuKey) + Integer.parseInt(item.get("totalActionTime").getN()));
					}else {
						b_execUser.put(orgMenuKey, getCount);
						b_execCont.put(orgMenuKey, Integer.parseInt(item.get("totalContCnt").getN()));
						b_execTime.put(orgMenuKey, Integer.parseInt(item.get("totalActionTime").getN()));
					}
				}
			}else {
				//편성메뉴 셋팅 (default 처리)
				for(int i=0; i<orgMenuCode.size(); i++) {
					String code = orgMenuCode.get(i).getOrgMenuCode();

					if(code.length() <= 5) {
						b_execUser.put(code, UUtil.nvl2("null"));
						b_execCont.put(code, UUtil.nvl2("null"));
						b_execTime.put(code, UUtil.nvl2("null"));
					}
				}
			}

			b_execTime.replaceAll((k, v) -> v / 60);
			resultMap.put("beforeDt", startDt);
			resultMap.put("beforeExecUser", b_execUser);
			resultMap.put("beforeExecCont", b_execCont);
			resultMap.put("beforeExecTime", b_execTime);
		}

		return resultMap;
	}

	/**
	 * 영상 콘텐츠 개별 통계
	 * @param startDt
	 * @param endDt
	 * @param contRankStat
	 * @param orgtMenuCodeList
	 * @param viewForm
	 * @return
	 */
	public List<Map<String, Object>> contRankStatTop(
			String startDt
			, String endDt
			, List<Map<String, Object>> contRankStat
			, List<CategoryZtreeVO> orgtMenuCodeList
			, String viewForm
			) throws Exception{

		if(AwsConfig.isAwsMode() == false) throw new Exception();

		String metaKey = "";
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		List<Map<String, AttributeValue>> putItem = new ArrayList<>();
		ScanRequest request = new ScanRequest();

		expressionAttributeValues.put(":metaStartDt", new AttributeValue().withS("Execute-" + startDt));
		expressionAttributeValues.put(":metaEndDt", new AttributeValue().withS("Execute-" + UUtil.getTmrDateYYYYMMDD(endDt)));

		String dynamoFilterExpr = "";

		if("UV".equals(viewForm)) {
			expressionAttributeValues.put(":zero", new AttributeValue().withN("0"));
			dynamoFilterExpr = "meta > :metaStartDt and meta < :metaEndDt and totalContCnt > :zero";
		}else if("PV".equals(viewForm)) {
			dynamoFilterExpr = "meta > :metaStartDt and meta < :metaEndDt";
		}

		do {
			request = new ScanRequest()
					.withTableName(dynamoDBTableName)
					.withFilterExpression(dynamoFilterExpr)
					.withExpressionAttributeValues(expressionAttributeValues)
					.withProjectionExpression("sid, meta, totalActionTime, totalContCnt, contNm, orgMenuCode")
					.withExclusiveStartKey(lastKeyEvaluated);
			
			ScanResult response = ddb.scan(request);
			for (Map<String, AttributeValue> item : response.getItems()){
		        putItem.add(item);
		    }
			lastKeyEvaluated = response.getLastEvaluatedKey();
		} while (null != lastKeyEvaluated);

		//시청 통계...
		Map<String, Integer> contCntMap = new LinkedHashMap<String, Integer>();
		Map<String, Integer> contExecMap = new HashMap<String, Integer>();
		Map<String, Integer> contTimeMap = new HashMap<String, Integer>();
		Map<String, String> contNmMap = new HashMap<String, String>();
		Map<String, String> orgMenuNmMap = new HashMap<String, String>();

		for (Map<String, AttributeValue> item : putItem) {
			metaKey = item.get("meta").getS().split("-")[2] + item.get("meta").getS().split("-")[3];
			if(contCntMap.containsKey(metaKey)) {
				contCntMap.put(metaKey, contCntMap.get(metaKey) + 1);
				contExecMap.put(metaKey, contExecMap.get(metaKey) + Integer.parseInt(item.get("totalContCnt").getN()));
				contTimeMap.put(metaKey, contTimeMap.get(metaKey) + Integer.parseInt(item.get("totalActionTime").getN()));
			}else {
				contCntMap.put(metaKey, 1);
				contExecMap.put(metaKey, Integer.parseInt(item.get("totalContCnt").getN()));
				contTimeMap.put(metaKey, Integer.parseInt(item.get("totalActionTime").getN()));

				contNmMap.put(metaKey, item.get("contNm").getS());
				orgMenuNmMap.put(metaKey, item.get("orgMenuCode").getS());
			}
		}

		contTimeMap.replaceAll((k, v) -> v / 60);

		int rankValue = 0;
		if("UV".equals(viewForm)) {
			contCntMap = sortMapByValue(contCntMap);
			for(String key : contCntMap.keySet()) {
				Map<String, Object> resultMap = new HashMap<String, Object>();
//				Optional<CategoryZtreeVO> categoryZtree = orgtMenuCodeList.stream().filter(s -> s.getOrgMenuCode().equals(orgMenuNmMap.get(key))).findFirst();
//				log.debug("kmh ::: " + categoryZtree.get().getOrgMenuNm());

				resultMap.put("rank", ++rankValue);
				resultMap.put("contOrgId", key);
				resultMap.put("contNm", contNmMap.get(key));
				resultMap.put("orgMenuNm", orgMenuNmMap.get(key));
				resultMap.put("execUser", contCntMap.get(key));
				resultMap.put("execCont", contExecMap.get(key));
				resultMap.put("execTime", contTimeMap.get(key));
				contRankStat.add(resultMap);
			}
		}else if("PV".equals(viewForm)) {
			contExecMap = sortMapByValue(contExecMap);
			for(String key : contExecMap.keySet()) {
				Map<String, Object> resultMap = new HashMap<String, Object>();
				resultMap.put("rank", ++rankValue);
				resultMap.put("contOrgId", key);
				resultMap.put("contNm", contNmMap.get(key));
				resultMap.put("orgMenuNm", orgMenuNmMap.get(key));
				resultMap.put("execUser", contCntMap.get(key));
				resultMap.put("execCont", contExecMap.get(key));
				resultMap.put("execTime", contTimeMap.get(key));
				contRankStat.add(resultMap);
			}
		}

		return contRankStat;
	}

	/**
	 * @param map
	 * @return
	 */
	public LinkedHashMap<String, Integer> sortMapByValue(Map<String, Integer> map) {
	    List<Map.Entry<String, Integer>> entries = new LinkedList<>(map.entrySet());
	    Collections.sort(entries, (o1, o2) -> o1.getValue().compareTo(o2.getValue()) * -1);

	    LinkedHashMap<String, Integer> result = new LinkedHashMap<>();

	    for (Map.Entry<String, Integer> entry : entries) {
	        result.put(entry.getKey(), entry.getValue());
	    }

	    return result;
	}

	/**
	 * @param monthDate
	 * @return
	 */
	public HashMap<String, Object> endOfMonthUser(String monthDate, HashMap<String, Object> endOfMonthMap) throws Exception{
		if(AwsConfig.isAwsMode() == false) throw new Exception();

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		List<Map<String, AttributeValue>> putItem = new ArrayList<>(); 
		String beforeMonth = UUtil.getBeforeDateYYYYMMDD(monthDate);
		String afterMonth = UUtil.getAfterDateYYYYMMDD(monthDate);
		ScanRequest request = new ScanRequest();

		expressionAttributeValues.put(":metaStart", new AttributeValue().withS(beforeMonth));
		expressionAttributeValues.put(":metaEnd", new AttributeValue().withS(afterMonth));

		do {
			request = new ScanRequest()
					.withTableName(dynamoDBTableName)
					.withFilterExpression("meta >= :metaStart and meta <= :metaEnd")
					.withExpressionAttributeValues(expressionAttributeValues)
					.withProjectionExpression("sid, meta")
					.withExclusiveStartKey(lastKeyEvaluated);
			
			ScanResult response = ddb.scan(request);
			for (Map<String, AttributeValue> item : response.getItems()){
		        putItem.add(item);
		    }
			lastKeyEvaluated = response.getLastEvaluatedKey();			
		} while (null != lastKeyEvaluated);

		
		Map<String, String> currentUserMap = new HashMap<String, String>();
		Map<String, String> beforeUserMap = new HashMap<String, String>();
		for (Map<String, AttributeValue> item : putItem) {
			String sidKey = item.get("sid").getS();
			String metaKey = item.get("meta").getS();
			if(metaKey.startsWith(monthDate)) {
				currentUserMap.put(sidKey, sidKey);
			}else if(metaKey.startsWith(beforeMonth)) {
				beforeUserMap.put(sidKey, sidKey);
			}
			
		}
		endOfMonthMap.put("monthUser"	, currentUserMap.size());
		endOfMonthMap.put("beforeMonthUser"	, beforeUserMap.size());
		return endOfMonthMap; 
	}

	/**
	 * @param monthDate
	 * @return
	 * @throws Exception
	 */
	public HashMap<String, Object> endOfMonthCont(String monthDate, HashMap<String, Object> endOfMonthMap) throws Exception{
		if(AwsConfig.isAwsMode() == false) throw new Exception();

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		List<Map<String, AttributeValue>> putItem = new ArrayList<>();
		String beforeMonth = UUtil.getBeforeDateYYYYMMDD(monthDate);
		String afterMonth = UUtil.getAfterDateYYYYMMDD(monthDate);
		ScanRequest request = new ScanRequest();

//		expressionAttributeValues.put(":meta", new AttributeValue().withS(monthDate));
		expressionAttributeValues.put(":metaStart", new AttributeValue().withS(beforeMonth));
		expressionAttributeValues.put(":metaEnd", new AttributeValue().withS(afterMonth));
		expressionAttributeValues.put(":zero", new AttributeValue().withN("0"));

		do {
			request = new ScanRequest()
					.withTableName(dynamoDBTableName)
//					.withFilterExpression("begins_with(meta, :meta) and totalContCntDaily > :zero")
					.withFilterExpression("meta >= :metaStart and meta <= :metaEnd and totalContCntDaily > :zero")
					.withExpressionAttributeValues(expressionAttributeValues)
					.withProjectionExpression("sid, meta, totalContCntDaily")
					.withExclusiveStartKey(lastKeyEvaluated);
			
			ScanResult response = ddb.scan(request);
			for (Map<String, AttributeValue> item : response.getItems()){
		        putItem.add(item);
		    }
			lastKeyEvaluated = response.getLastEvaluatedKey();
		} while (null != lastKeyEvaluated);

		Map<String, String> currentUserMap = new HashMap<String, String>();
		Map<String, String> beforeUserMap = new HashMap<String, String>();
		int currentResultCnt = 0;
		int beforeResultCnt = 0;
		for (Map<String, AttributeValue> item : putItem) {
			String sidKey = item.get("sid").getS();
			String metaKey = item.get("meta").getS();
			if(metaKey.startsWith(monthDate)) {
				currentUserMap.put(sidKey, sidKey);
				currentResultCnt += Integer.parseInt(item.get("totalContCntDaily").getN());
			}else if(metaKey.startsWith(beforeMonth)) {
				beforeUserMap.put(sidKey, sidKey);
				beforeResultCnt += Integer.parseInt(item.get("totalContCntDaily").getN());
			}
		}

		endOfMonthMap.put("execUser"	, currentUserMap.size());
		endOfMonthMap.put("execCont"	, currentResultCnt);
		endOfMonthMap.put("beforeExecUser"	, beforeUserMap.size());
		endOfMonthMap.put("beforeExecCont"	, beforeResultCnt);
		return endOfMonthMap;
	}

	/**
	 * @param monthDate
	 * @return
	 * @throws Exception
	 */
	@Deprecated
	public int endOfMonthContExec(String monthDate) throws Exception {
		if(AwsConfig.isAwsMode() == false) throw new Exception();

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		List<Map<String, AttributeValue>> putItem = new ArrayList<>(); 
		ScanRequest request = new ScanRequest();

		expressionAttributeValues.put(":meta", new AttributeValue().withS(monthDate));
		expressionAttributeValues.put(":zero", new AttributeValue().withN("0"));

		do {
			request = new ScanRequest()
					.withTableName(dynamoDBTableName)
					.withFilterExpression("begins_with(meta, :meta) and totalContCntDaily > :zero")
					.withExpressionAttributeValues(expressionAttributeValues)
					.withProjectionExpression("sid, totalContCntDaily")
					.withExclusiveStartKey(lastKeyEvaluated);
			
			ScanResult response = ddb.scan(request);
			for (Map<String, AttributeValue> item : response.getItems()){
		        putItem.add(item);
		    }
			lastKeyEvaluated = response.getLastEvaluatedKey();
		} while (null != lastKeyEvaluated);

		int resultCnt = 0;
		for (Map<String, AttributeValue> item : putItem) {
			resultCnt += Integer.parseInt(item.get("totalContCntDaily").getN());
		}

		return resultCnt;
	}

	/**
	 * @param monthDate
	 * @return
	 * @throws Exception
	 */
	public HashMap<String, Object> endOfMonthExecTime(String monthDate, HashMap<String, Object> endOfMonthMap) throws Exception {
		if(AwsConfig.isAwsMode() == false) throw new Exception();

		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		Map<String, AttributeValue> lastKeyEvaluated = null;
		List<Map<String, AttributeValue>> putItem = new ArrayList<>();
		String beforeMonth = UUtil.getBeforeDateYYYYMMDD(monthDate);
		String afterMonth = UUtil.getAfterDateYYYYMMDD(monthDate);
		ScanRequest request = new ScanRequest();

//		expressionAttributeValues.put(":meta", new AttributeValue().withS("Execute-" + monthDate));
		
		expressionAttributeValues.put(":metaStart", new AttributeValue().withS("Execute-" + beforeMonth));
		expressionAttributeValues.put(":metaEnd", new AttributeValue().withS("Execute-" + afterMonth));

		do {
			request = new ScanRequest()
					.withTableName(dynamoDBTableName)
//					.withFilterExpression("begins_with(meta, :meta)")
					.withFilterExpression("meta >= :metaStart and meta <= :metaEnd")
					.withExpressionAttributeValues(expressionAttributeValues)
					.withProjectionExpression("sid, meta, totalActionTime")
					.withExclusiveStartKey(lastKeyEvaluated);
			
			ScanResult response = ddb.scan(request);
			for (Map<String, AttributeValue> item : response.getItems()){
		        putItem.add(item);
		    }
			lastKeyEvaluated = response.getLastEvaluatedKey();
		} while (null != lastKeyEvaluated);

		int currentResultTime = 0;
		int beforeResultTime = 0;
		for (Map<String, AttributeValue> item : putItem) {
			String metaKey = item.get("meta").getS();
			if(metaKey.startsWith("Execute-" + monthDate)) {
				currentResultTime += Integer.parseInt(item.get("totalActionTime").getN());
			}else if(metaKey.startsWith("Execute-" + beforeMonth)) {
				beforeResultTime += Integer.parseInt(item.get("totalActionTime").getN());
			}
		}

		endOfMonthMap.put("execTime"	, currentResultTime / 60);
		endOfMonthMap.put("beforeExecTime"	, beforeResultTime / 60);
		return endOfMonthMap;
	}

	/**
	 * @param endOfMonthMap 
	 * @param mont
	 * @return
	 */
	public HashMap<String, Object> keyStatCurrent(String monthDate, HashMap<String, Object> endOfMonthMap) throws Exception{
		if(AwsConfig.isAwsMode() == false) throw new Exception();
		
		endOfMonthMap.put("date", monthDate);
		endOfMonthMap.put("beforeDate", UUtil.getBeforeDateYYYYMMDD(monthDate));
		endOfMonthMap = endOfMonthUser(monthDate, endOfMonthMap);
		endOfMonthMap = endOfMonthCont(monthDate, endOfMonthMap); 
		endOfMonthMap = endOfMonthExecTime(monthDate, endOfMonthMap);
		
		return endOfMonthMap;
	}


}
