package com.serverless.lambda.function;

import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBRangeKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class GetTopTen implements RequestHandler<Object, String> {
	
	static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
    static DynamoDB dynamoDB = new DynamoDB(client);
	static String tableName = "images";
	@DynamoDBTable(tableName = "images")
	public static class EventImage{

	        private String imageName;
	        private int totalAccess;
	        private double value;
	        
	        @DynamoDBHashKey(attributeName = "imageName")
			public String getImageName() {
				return imageName;
			}
			public void setImageName(String imageName) {
				this.imageName = imageName;
			}
			
			@DynamoDBRangeKey(attributeName = "totalAccess")
			public int getTotalAccess() {
				return totalAccess;
			}
			public void setTotalAccess(int totalAccess) {
				this.totalAccess = totalAccess;
			}
			public double getValue() {
				return value;
			}
			public void setValue(double value) {
				this.value = value;
			}
			
	}
	
	@Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
        ObjectMapper objMapper = new ObjectMapper();
        Table table = dynamoDB.getTable("tableName");
        Index index = table.getIndex("TotalAccessIndex");
        objMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        
        try {
        	DynamoDBMapper mapper = new DynamoDBMapper(client);
        	DynamoDBScanExpression scanExpression = new DynamoDBScanExpression();      
        	scanExpression.setIndexName("TotalAccessIndex");
            List<EventImage> scanResult = mapper.scan(EventImage.class, scanExpression);            
            System.out.println("Scan of " + tableName + " for items.");
            StringBuilder result = new StringBuilder("");
            for (EventImage img: scanResult){
            	img.setValue(img.getTotalAccess() * 0.05);
            	System.out.println("Imagem "+ img.getImageName() + " Acessos: " + img.getTotalAccess() + " Custo: R$" + img.getValue());
            	String jsonInString =objMapper.writeValueAsString(img);
            	result.append(jsonInString);
            }            
           
            return result.toString();
        } catch (Exception e) {
            context.getLogger().log(String.format(
                "Error ao obter itens na tabela  %s ", tableName));   
            e.printStackTrace();
            return String.format("Error ao obter itens na tabela  %s ", tableName);
        }

    }

}
