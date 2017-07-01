package com.serverless.lambda.function;

import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class InsertMusic implements RequestHandler<Object, String> {

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
        	
        	Item item = new Item()
        	    .withPrimaryKey("Artist", mapInput.get("Artist"))
        	    .withString("SongTitle", mapInput.get("SongTitle"))
        	    .withString("AlbumTitle", mapInput.get("AlbumTitle"))
        	    .withNumber("Year", Integer.parseInt(mapInput.get("Year")));

        	// Write the item to the table 
        	PutItemOutcome outcome = table.putItem(item);
                        
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error ao inserir item na tabela  %s ", tableName)); 
            return String.format(
                    "Error ao inserir item na tabela  %s ", tableName);
        }
        return "OK";
    }

}
