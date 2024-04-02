package com.chtrembl.petstore;

import com.azure.core.http.HttpResponse;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.ServiceBusTopicTrigger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.logging.Level;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {

    public static final int RETRY_COUNT = 3;

    @FunctionName("reserveorder")
    public void run(
            @ServiceBusTopicTrigger(
                    name = "orders",
                    topicName = "orders",
                    subscriptionName = "functionapp",
                    connection = "SERVICE_BUS_ORDERS_TOPIC_CONNECTION_STRING"
            ) String message,
            final ExecutionContext context) {
        context.getLogger().info("Java Service Bus trigger processed a message: " + message);

        String orderId = getOrderIdFromMessage(message, context);

        boolean uploaded = false;
        for (int i = 0; i < RETRY_COUNT; i++) {
            uploaded = tryUploadToBlob(message, context, orderId);
            if (uploaded) {
                break;
            }
        }
        if (!uploaded) {
            int statusCode = uploadToLogicApps(message, context);
            if (statusCode >= 200 && statusCode < 300) {
                context.getLogger().info("Successfully triggered fallback scenario. Sending order details to email.");
            } else {
                context.getLogger().log(Level.SEVERE, "Fallback scenario fails: " + statusCode);
            }
        }
    }

    private int uploadToLogicApps(String body, ExecutionContext context) {
        String logicAppUrl = System.getenv("LOGIC_APP_URL");
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(logicAppUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        java.net.http.HttpResponse response = null;
        try {
            response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString());
        } catch (IOException | InterruptedException e) {
            context.getLogger().log(Level.SEVERE, "Failed to send the request to Logic App");
            throw new RuntimeException(e);
        }
        return response.statusCode();
    }

    private static boolean tryUploadToBlob(String message, ExecutionContext context, String orderId) {
        String connectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
        try {
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
            BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient("reservationcontainer");
            BlobClient blobClient = blobContainerClient.getBlobClient(orderId + ".json");
            blobClient.upload(BinaryData.fromString(message), true);
            return true;
        } catch (BlobStorageException e) {
            HttpResponse response = e.getResponse();
            context.getLogger().log(Level.SEVERE, "Uploading failed with Http status code: " + response.getStatusCode());
            if (e.getErrorCode() == BlobErrorCode.RESOURCE_NOT_FOUND) {
                context.getLogger().log(Level.SEVERE, "Extended details: " + e.getStatusCode());
            } else if (e.getErrorCode() == BlobErrorCode.CONTAINER_BEING_DELETED) {
                context.getLogger().log(Level.SEVERE, "Extended details: " + e.getServiceMessage());
            }
            return false;
        }
    }

    private static String getOrderIdFromMessage(String message, ExecutionContext context) {
        JsonNode jsonNode = null;
        try {
            jsonNode = new ObjectMapper().readTree(message);
        } catch (JsonProcessingException e) {
            context.getLogger().log(Level.SEVERE, "Cannot parse message from service bus: " + e.getMessage());
            throw new RuntimeException(e);
        }
        return jsonNode.get("id").asText();
    }
}
