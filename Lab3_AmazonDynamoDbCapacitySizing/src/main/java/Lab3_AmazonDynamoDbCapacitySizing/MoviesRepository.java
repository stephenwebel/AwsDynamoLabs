package Lab3_AmazonDynamoDbCapacitySizing;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.services.applicationautoscaling.ApplicationAutoScalingClient;
import software.amazon.awssdk.services.applicationautoscaling.model.MetricType;
import software.amazon.awssdk.services.applicationautoscaling.model.PolicyType;
import software.amazon.awssdk.services.applicationautoscaling.model.ServiceNamespace;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

@Requires(beans = {DynamoRepository.class})
@Singleton
public class MoviesRepository {

  private static final String tableName = "movies";
  private static final String HK = "year";
  private static final String SK = "title";

  private final String policyName = "MyScalingPolicy";
  private final String resourceId = "table/" + tableName;
  private final String scalableDimRead = "dynamodb:table:ReadCapacityUnits";
  private final String scalableDimWrite = "dynamodb:table:WriteCapacityUnits";


  private final DynamoDbClient dynamoDbClient;
  private final ApplicationAutoScalingClient aaClient;
  private final DynamoRepository dynamoRepo;


  @Inject
  public MoviesRepository(DynamoRepository dynamoRepository) {
    this.dynamoRepo = dynamoRepository;
    this.dynamoDbClient = dynamoRepository.getDynamoDbClient();
    this.aaClient = dynamoRepository.getApplicationAutoScalingClient();
  }

  String getMoviesTableStatus() {
    return dynamoRepo.checkStatus(tableName).toString();
  }
  public void queryItem() {
    Map<String, AttributeValue> key = new HashMap<>();
    key.put(HK, AttributeValue.builder().n("2013").build());
    key.put(SK, AttributeValue.builder().s("Gravity").build());

    GetItemRequest getItemRequest = GetItemRequest.builder()
        .tableName(tableName)
        .key(key)
        .build();

    try {
      GetItemResponse getItemResponse = dynamoDbClient.getItem(getItemRequest);
      System.out.println("Item retrieved: " + getItemResponse.item());
    } catch (Exception e) {
      System.out.println(e.getMessage());
    }
  }

  public void updateMoviesTableCapacity() {
    UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
        .tableName(tableName)
        .provisionedThroughput(ProvisionedThroughput.builder()
                                   .readCapacityUnits(2L)
                                   .writeCapacityUnits(2L)
                                   .build())
        .build();

    dynamoDbClient.updateTable(updateTableRequest);
  }

  public void disableAutoScaling() {
    deleteAutoScalePolicyRead();
    deleteAutoScalePolicyWrite();
    deregisterAutoScaleTargetRead();
    deregisterAutoScaleTargetWrite();
  }

  public void enableAutoScaling() {
    registerAutoScaleTargetRead();
    registerAutoScaleTargetWrite();
    putAutoScalePolicyRead();
    putAutoScalePolicyWrite();
  }

  // Corresponding methods for disabling auto-scaling
  private void deregisterAutoScaleTargetRead() {
    aaClient.deregisterScalableTarget(builder -> builder
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceId)
        .scalableDimension(scalableDimRead));
  }

  private void deleteAutoScalePolicyRead() {
    aaClient.deleteScalingPolicy(builder -> builder
        .policyName(policyName)
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceId)
        .scalableDimension(scalableDimRead));
  }

  private void deregisterAutoScaleTargetWrite() {
    aaClient.deregisterScalableTarget(builder -> builder
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceId)
        .scalableDimension(scalableDimWrite));
  }

  private void deleteAutoScalePolicyWrite() {
    aaClient.deleteScalingPolicy(builder -> builder
        .policyName(policyName)
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceId)
        .scalableDimension(scalableDimWrite));
  }

  // Corresponding methods for enabling auto-scaling
  private void registerAutoScaleTargetRead() {
    aaClient.registerScalableTarget(builder -> builder
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceId)
        .scalableDimension(scalableDimRead)
        .minCapacity(1)
        .maxCapacity(40000));
  }

  private void putAutoScalePolicyRead() {
    aaClient.putScalingPolicy(builder -> builder
        .policyName(policyName)
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceId)
        .scalableDimension(scalableDimRead)
        .policyType(PolicyType.TARGET_TRACKING_SCALING)
        .targetTrackingScalingPolicyConfiguration(config -> config
            .targetValue(70.0)
            .predefinedMetricSpecification(spec -> spec
                .predefinedMetricType(MetricType.DYNAMO_DB_READ_CAPACITY_UTILIZATION))
            .scaleOutCooldown(60)
            .scaleInCooldown(60)));
  }

  private void registerAutoScaleTargetWrite() {
    aaClient.registerScalableTarget(builder -> builder
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceId)
        .scalableDimension(scalableDimWrite)
        .minCapacity(1)
        .maxCapacity(40000));
  }

  private void putAutoScalePolicyWrite() {
    aaClient.putScalingPolicy(builder -> builder
        .policyName(policyName)
        .serviceNamespace(ServiceNamespace.DYNAMODB)
        .resourceId(resourceId)
        .scalableDimension(scalableDimWrite)
        .policyType(PolicyType.TARGET_TRACKING_SCALING)
        .targetTrackingScalingPolicyConfiguration(config -> config
            .targetValue(70.0)
            .predefinedMetricSpecification(spec -> spec
                .predefinedMetricType(MetricType.DYNAMO_DB_WRITE_CAPACITY_UTILIZATION))
            .scaleOutCooldown(60)
            .scaleInCooldown(60)));
  }

}
