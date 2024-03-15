package com.chtrembl.petstore;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.HttpMethod;
import com.microsoft.azure.functions.HttpRequestMessage;
import com.microsoft.azure.functions.HttpResponseMessage;
import com.microsoft.azure.functions.HttpStatus;
import com.microsoft.azure.functions.annotation.AuthorizationLevel;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.HttpTrigger;

import java.util.Optional;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {
    @FunctionName("reserveorder")
    public HttpResponseMessage run(
            @HttpTrigger(
                name = "req",
                methods = {HttpMethod.POST},
                authLevel = AuthorizationLevel.ANONYMOUS)
                HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {
        context.getLogger().info("Java HTTP trigger processed a request.");

        final String bodyJson = request.getBody().orElse("Empty");
        context.getLogger().info("Body: " + request.getBody());
        
        String connectionString = System.getenv("AZURE_STORAGE_CONNECTION_STRING");

        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder().connectionString(connectionString).buildClient();
        BlobContainerClient blobContainerClient = blobServiceClient.getBlobContainerClient("reservationcontainer");
        BlobClient blobClient = blobContainerClient.getBlobClient("reservation.json");
        blobClient.upload(BinaryData.fromString(bodyJson), true);

        if (bodyJson.equals("Empty")) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST).body("Please pass data in the request body").build();
        } else {
            return request.createResponseBuilder(HttpStatus.OK).body("Success").build();
        }
    }
}
