package com.chtrembl.petstore;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.ServiceBusTopicTrigger;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {

    @FunctionName("reserveorder")
    public void run(
            @ServiceBusTopicTrigger(
                    name = "orders",
                    topicName = "orders",
                    subscriptionName = "functionapp",
                    connection = "SERVICE_BUS_ORDERS_TOPIC_CONNECTION_STRING"
            ) String message,
            final ExecutionContext context) {
        context.getLogger().info("Java Service Bus trigger processed a message.");
        context.getLogger().info("Message: " + message);
        
        String connectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING");

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient("reservationcontainer");
        BlobClient blobClient = blobContainerClient.getBlobClient("reservation.json");
        blobClient.upload(BinaryData.fromString(message), true);
    }
}
