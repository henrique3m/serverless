package com.serverless.lambda.function;

import java.util.HashMap;
import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class GetByTitle implements RequestHandler<Object, String> {
	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    static DynamoDB dynamoDB = new DynamoDB(client);
    static String tableName = "Music";
    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
        String song = input.toString();
        
        Table table = dynamoDB.getTable(tableName);
        Index index = table.getIndex("SongTitleIndex");
        try {
            HashMap<String, String> nameMap = new HashMap<String, String>();
            nameMap.put("#y", "SongTitle");

            HashMap<String, Object> valueMap = new HashMap<String, Object>();
            valueMap.put(":x", song);

            QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#y = :x").withNameMap(nameMap)
                .withValueMap(valueMap);

            ItemCollection<QueryOutcome> items = index.query(querySpec);
            Iterator<Item> iter = items.iterator();
            StringBuilder result = new StringBuilder("");
            while (iter.hasNext()) {
               	Item item = iter.next();
                System.out.println(item.toJSONPretty());
                result.append(item.toJSONPretty());
            }
            return result.toString();
                        
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error ao pesquisar pelo artista:  %s na tabela  %s ", song, tableName));
            throw e;
        }
    }
}
