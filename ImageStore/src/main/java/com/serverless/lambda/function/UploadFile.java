package com.serverless.lambda.function;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;

public class UploadFile implements RequestHandler<S3Event, String> {

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    static DynamoDB dynamoDB = new DynamoDB(client);

    static String tableName = "images";

    public UploadFile() {}

    // Test purpose only.
    UploadFile(AmazonS3 s3) {
    }

    //Trigger by S3 event
    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);
        
        String key = event.getRecords().get(0).getS3().getObject().getKey();
        if (!key.contains(".jpg") && !key.contains(".png"))
        	return "Upload não é imagem";
        try {
        	
            Table table = dynamoDB.getTable(tableName);
            Item item = new Item().withPrimaryKey("imageName", key);
            table.putItem(item);
            return "Image " + key + " uploaded";
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error ao incluir item  %s na tabela  %s ", key, tableName));
            throw e;
        }
    }
}