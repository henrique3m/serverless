package com.serverless.lambda.function;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class UpdateMusic implements RequestHandler<Object, String> {

	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    static DynamoDB dynamoDB = new DynamoDB(client);
    static String tableName = "Music";
       
    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
		if (input.toString().equals("{}") || input.toString().equals("")) {
			context.getLogger().log("input is empty: abort");
			return "{\"status\":\"error\",\"message\":\"input at lambda function is empty\"}";
		}
		
        HashMap<String, String> mapInput = (HashMap<String, String>) input;
        
        Table table = dynamoDB.getTable(tableName);
        try {        
        	       	
        	String album = mapInput.get("AlbumTitle");
        	if (!album.isEmpty()) {
        		 Map<String, String> expressionAttributeNames = new HashMap<String, String>();
             	expressionAttributeNames.put("#B", "AlbumTitle");
             	 HashMap<String, Object> valueMap = new HashMap<String, Object>();
                 valueMap.put(":b",  album);
                 UpdateItemOutcome outcome =  table.updateItem(
                		 new PrimaryKey("Artist", mapInput.get("Artist"), "SongTitle", mapInput.get("SongTitle")),           // key attribute value
                 	    "set #B=:b", // UpdateExpression
                 	    expressionAttributeNames,
                 	    valueMap);
        	}

         	String year = mapInput.get("Year");
        	if (!year.isEmpty()) {
        		 Map<String, String> expressionAttributeNames = new HashMap<String, String>();
             	expressionAttributeNames.put("#C", "Year");
             	 HashMap<String, Object> valueMap = new HashMap<String, Object>();
                 valueMap.put(":c", Integer.parseInt(year));
                 UpdateItemOutcome outcome =  table.updateItem(
                		 new PrimaryKey("Artist", mapInput.get("Artist"), "SongTitle", mapInput.get("SongTitle")),           // key attribute value
                 	    "set #C=:c", // UpdateExpression
                 	    expressionAttributeNames,
                 	    valueMap);
        	}        	        	       
                        
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error ao atualizar item na tabela  %s ", tableName)); 
            return String.format(
                    "Error ao atualizar item na tabela  %s ", tableName);
        }
        return "Item atualizado com sucesso";
    }

}
