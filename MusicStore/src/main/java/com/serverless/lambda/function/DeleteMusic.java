package com.serverless.lambda.function;

import java.util.HashMap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class DeleteMusic implements RequestHandler<Object, String> {

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
        	// Write the item to the table 
        	DeleteItemSpec spec = new DeleteItemSpec().withPrimaryKey(new PrimaryKey("Artist", mapInput.get("Artist"), "SongTitle", mapInput.get("SongTitle")));
        	table.deleteItem(spec);
                        
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error ao excluir item %s na tabela  %s ", mapInput.get("Artist"), tableName)); 
            return String.format(
                    "Error ao excluir item %s na tabela  %s ", mapInput.get("Artist"), tableName);
        }
        return "Item excluido com sucesso";
    }

}
