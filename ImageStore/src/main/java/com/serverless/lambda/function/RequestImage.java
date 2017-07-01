package com.serverless.lambda.function;

import java.util.HashMap;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;

public class RequestImage implements RequestHandler<Object, String> {
	
	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    static DynamoDB dynamoDB = new DynamoDB(client);

    static String tableName = "images";
    static int access = 0;
    
    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
        String imageName = input.toString();
        Table table = dynamoDB.getTable(tableName);
        
        try {
            HashMap<String, String> nameMap = new HashMap<String, String>();
            nameMap.put("#y", "imageName");

            HashMap<String, Object> valueMap = new HashMap<String, Object>();
            valueMap.put(":x", imageName);

            QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#y = :x").withNameMap(nameMap)
                .withValueMap(valueMap);

            ItemCollection<QueryOutcome> items = null;
            Iterator<Item> iterator = null;
            Item item = null;
            items = table.query(querySpec);

            iterator = items.iterator();
            while (iterator.hasNext()) {
                item = iterator.next();
                access = item.getInt("totalAccess");    
            }
            
            access ++;
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error ao obter item  %s na tabela  %s ", imageName, tableName));
            throw e;
        }
        
        UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("imageName", imageName)
                .withUpdateExpression("set totalAccess = :r")
                .withValueMap(new ValueMap().withInt(":r", access))
                .withReturnValues(ReturnValue.UPDATED_NEW);

            try {
                System.out.println("Updating the item...");
                UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
                System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

            }
            catch (Exception e) {
                System.err.println("Unable to update item: " + imageName);
                System.err.println(e.getMessage());
            }
            
        return "OK";
    }

}
