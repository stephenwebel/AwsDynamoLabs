package Lab3_AmazonDynamoDbCapacitySizing;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.net.URI;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;

@Requires(beans = {DynamoConfiguration.class, DynamoDbClient.class})
@Singleton
public class DynamoRepository {

  private final DynamoDbClient dynamoDbClient;
  private final ApplicationAutoScalingClient applicationAutoScalingClient;

  public DynamoRepository(DynamoConfiguration dynamoConfiguration) {
    this.dynamoDbClient = DynamoDbClient.builder()
        .region(Region.of(dynamoConfiguration.getRegion())) // or your desired region
        .endpointOverride(URI.create(dynamoConfiguration.getEndpoint()))
        .build();
     this.applicationAutoScalingClient = ApplicationAutoScalingClient.builder()
        .region(Region.of(dynamoConfiguration.getRegion())) // or your desired region
        .endpointOverride(URI.create(dynamoConfiguration.getEndpoint()))
        .build();
  }

  public DynamoDbClient getDynamoDbClient() {
    return dynamoDbClient;
  }

  public ApplicationAutoScalingClient getApplicationAutoScalingClient() {
    return applicationAutoScalingClient;
  }

  public DeleteTableResponse removeTable(String tableName) {
    DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
        .tableName(tableName)
        .build();

    return dynamoDbClient.deleteTable(deleteTableRequest);
  }

  DescribeTableResponse checkStatus(String tableName) {
    DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
        .tableName(tableName)
        .build();

    return dynamoDbClient.describeTable(describeTableRequest);
  }
}
