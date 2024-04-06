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
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.util.logging.Level;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {

    private static final int RETRY_COUNT = 3;
    private static final MediaType JSON = MediaType.get("application/json");

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
        tryUploadToBlob(message, context, orderId);
    }

    private static void tryUploadToBlob(String message, ExecutionContext context, String orderId) {
        String connectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING");
        try {
            BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
            BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient("reservationcontainer");
            BlobClient blobClient = blobContainerClient.getBlobClient(orderId + ".json");
            blobClient.upload(BinaryData.fromString(message), true);
        } catch (BlobStorageException e) {
            HttpResponse response = e.getResponse();
            context.getLogger().log(Level.SEVERE, "Uploading failed with Http status code: " + response.getStatusCode());
            if (e.getErrorCode() == BlobErrorCode.RESOURCE_NOT_FOUND) {
                context.getLogger().log(Level.SEVERE, "Extended details: " + e.getStatusCode());
            } else if (e.getErrorCode() == BlobErrorCode.CONTAINER_BEING_DELETED) {
                context.getLogger().log(Level.SEVERE, "Extended details: " + e.getServiceMessage());
            }
            throw new RuntimeException(e);
        } catch (Exception e) {
            context.getLogger().log(Level.SEVERE, "The issue happened during uploading to blob: " + e.getMessage());
            throw new RuntimeException(e);
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
