package Lab2_AmazonDynamoDBScansQueriesandIndexes;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.io.FileReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@Requires(condition = CIAwsRegionProviderChainCondition.class)
@Requires(condition = CIAwsCredentialsProviderChainCondition.class)
@Requires(beans = {DynamoConfiguration.class, DynamoDbClient.class})
@Singleton
public class DynamoRepository {

  private final DynamoDbClient dynamoDbClient;

  public DynamoRepository() {
    this.dynamoDbClient = DynamoDbClient.builder()
        .region(Region.US_EAST_1) // or your desired region
        .endpointOverride(URI.create("http://localhost:9000"))
        .build();
  }

  public DynamoDbClient getDynamoDbClient() {
    return dynamoDbClient;
  }

  public CreateTableResponse createTable(String tableName) {
    CreateTableRequest request = CreateTableRequest.builder()
        .tableName(tableName)
        .keySchema(
            KeySchemaElement.builder().attributeName("year").keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName("title").keyType(KeyType.RANGE).build()
        )
        .attributeDefinitions(
            AttributeDefinition.builder().attributeName("year").attributeType(ScalarAttributeType.N).build(),
            AttributeDefinition.builder().attributeName("title").attributeType(ScalarAttributeType.S).build(),
            AttributeDefinition.builder().attributeName("genre").attributeType(ScalarAttributeType.S).build()
        )
        .localSecondaryIndexes(
            LocalSecondaryIndex.builder()
                .indexName("genre-index")
                .keySchema(
                    KeySchemaElement.builder().attributeName("year").keyType(KeyType.HASH).build(),
                    KeySchemaElement.builder().attributeName("genre").keyType(KeyType.RANGE).build()
                )
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                .build()
        )
        .provisionedThroughput(
            ProvisionedThroughput.builder()
                .readCapacityUnits(1000L)
                .writeCapacityUnits(1000L)
                .build()
        )
        .build();

    CreateTableResponse response = dynamoDbClient.createTable(request);
    return response;
  }

  public DeleteTableResponse removeTable(String tableName) {
    DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
        .tableName(tableName)
        .build();

    return dynamoDbClient.deleteTable(deleteTableRequest);
  }

  String checkStatus(String tableName) {
    long startTime = System.currentTimeMillis();

    DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
        .tableName(tableName)
        .build();

    DescribeTableResponse response = dynamoDbClient.describeTable(describeTableRequest);
    String tableStatus = response.table().tableStatus().toString();

    long endTime = System.currentTimeMillis();
    double totalTime = (endTime - startTime) / 1000.0;

    System.out.println("Status of table is: " + tableStatus);
    System.out.println("Total time: " + totalTime + " sec");
    return tableStatus;
  }


}
