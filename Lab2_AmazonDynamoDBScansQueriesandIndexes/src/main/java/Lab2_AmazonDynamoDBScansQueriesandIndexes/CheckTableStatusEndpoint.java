package Lab2_AmazonDynamoDBScansQueriesandIndexes;

import io.micronaut.http.MediaType;
import io.micronaut.management.endpoint.annotation.Endpoint;
import io.micronaut.management.endpoint.annotation.Read;
import io.micronaut.management.endpoint.annotation.Selector;
import io.micronaut.management.endpoint.annotation.Write;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;

@Endpoint(value = "status", defaultSensitive = false)
public class CheckTableStatusEndpoint {

  final DynamoRepository dynamoRepository;

  @Inject
  public CheckTableStatusEndpoint(DynamoRepository dynamoRepository) {
    this.dynamoRepository = dynamoRepository;
  }

  @Write(produces = MediaType.APPLICATION_JSON)
  public String createTable(@Selector String tableName) {
    CreateTableResponse table = dynamoRepository.createTable(tableName);
    return table.toString();
  }

  @Read(produces = MediaType.TEXT_PLAIN)
  public String checkStatus(@Selector String tableName) {
    return dynamoRepository.checkStatus(tableName);
  }

}
