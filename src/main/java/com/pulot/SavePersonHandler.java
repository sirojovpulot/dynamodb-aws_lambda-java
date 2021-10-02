package com.pulot;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class SavePersonHandler implements RequestStreamHandler {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("US-ASCII")));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(outputStream, Charset.forName("US-ASCII"))));
        LambdaLogger logger = context.getLogger();
        try {
            HashMap event = gson.fromJson(reader, HashMap.class);
            PersonRequest body = gson.fromJson(event.get("body").toString(), PersonRequest.class);
            Object routeKey = event.get("routeKey");
            if ("POST /items".equals(routeKey)) {
                createItems(body);
                writer.write(gson.toJson(body));
            } else if ("GET /items".equals(routeKey)) {
                PaginatedScanList<Person> items = getItems();
                writer.write(gson.toJson(items));
            } else if ("DELETE /items/{id}".equals(routeKey)) {
                Map pathParameters = (Map) event.get("pathParameters");
                PersonResponse response = deleteItem(pathParameters.get("id").toString());
                writer.write(gson.toJson(response));
            } else {
                throw new Exception("Unsupported route");
            }

            if (writer.checkError()) {
                logger.log("WARNING: Writer encountered an error.");
            }
        } catch (IllegalStateException | JsonSyntaxException exception) {
            logger.log(exception.toString());
        } catch (Exception e) {
            logger.log(e.toString());
        } finally {
            reader.close();
            writer.close();
        }
    }

    private PersonResponse deleteItem(String id) {
        DynamoDBMapper mapper = new DynamoDBMapper(client);
        Person person = mapper.load(Person.class, id);
        if (person != null) {
            mapper.delete(person);
            return new PersonResponse("deleted");
        } else {
            return new PersonResponse("item does not exist");
        }
    }

    private PaginatedScanList<Person> getItems() {
        DynamoDBMapper mapper = new DynamoDBMapper(client);
        return mapper.scan(Person.class, new DynamoDBScanExpression());
    }

    private PersonResponse createItems(PersonRequest personRequest) {

        try {
            Person item = new Person();
            item.setId(personRequest.getId());
            item.setFirstName(personRequest.getFirstName());
            item.setLastName(personRequest.getLastName());
            DynamoDBMapper mapper = new DynamoDBMapper(client);
            mapper.save(item);
            return new PersonResponse("success");
        } catch (Exception e) {
            return new PersonResponse(e.getLocalizedMessage());
        }
    }

    @DynamoDBTable(tableName = "http-crud-tutorial-items")
    public static class Person {
        private String id;
        private String firstName;
        private String lastName;

        // Partition key
        @DynamoDBHashKey(attributeName = "id")
        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        @DynamoDBAttribute(attributeName = "firstName")
        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }

        @DynamoDBAttribute(attributeName = "lastName")
        public String getLastName() {
            return lastName;
        }

        public void setLastName(String lastName) {
            this.lastName = lastName;
        }

        @Override
        public String toString() {
            return "Person [firstName=" + firstName + ", lastName=" + lastName + "]";
        }
    }

}
