package com.serverless.lambda.function;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;

public class DeleteFile implements RequestHandler<S3Event, String> {

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    static DynamoDB dynamoDB = new DynamoDB(client);

    static String tableName = "images";
    public DeleteFile() {}

    // Test purpose only.
    DeleteFile(AmazonS3 s3) {
    }

    //Trigger by S3 event
    @Override
    public String handleRequest(S3Event event, Context context) {
        context.getLogger().log("Received event: " + event);

        // Get the object from the event and show its content type
        String key = event.getRecords().get(0).getS3().getObject().getKey();
        if (!key.contains(".jpg") && !key.contains(".png"))
        	return "Arquivo não é uma imagem";
        try {
        	 Table table = dynamoDB.getTable(tableName);
             DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                     .withPrimaryKey(new PrimaryKey("imageName", key));
             table.deleteItem(deleteItemSpec);
             return "Image " + key + " deleted";
             
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
            		 "Error ao excluir item  %s na tabela  %s ", key, tableName));
            throw e;
        }
    }
}